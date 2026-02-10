package com.minare.core.transport

import com.minare.application.config.FrameworkConfig
import com.minare.cache.ConnectionCache
import com.minare.core.storage.interfaces.ConnectionStore
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import com.minare.core.transport.upsocket.UpSocketVerticle
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType

/**
 * Verticle that handles periodic cleaning of stale connections and data.
 * This helps ensure system health by removing orphaned resources.
 */
class CleanupVerticle @Inject constructor(
    private val frameworkConfig: FrameworkConfig,
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache,
    private val debug: DebugLogger
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(CleanupVerticle::class.java)

    companion object {
        const val ADDRESS_TRIGGER_CLEANUP = "minare.trigger.cleanup"
    }

    private val reconnectTimeout = frameworkConfig.sockets.connection.reconnectTimeout
    private val cleanupInterval = frameworkConfig.sockets.connection.cleanupInterval
    private val connectionExpiry = frameworkConfig.sockets.connection.connectionExpiry
    private val aggressiveCleanup = frameworkConfig.sockets.connection.aggressiveCleanup

    override suspend fun start() {
        log.info("Starting CleanupVerticle")

        vertx.setPeriodic(cleanupInterval) { _ ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val result = performCleanup(aggressiveCleanup)

                    debug.log(DebugType.CLEANUP_VERTICLE_METRICS, listOf(result.first, result.second))
                } catch (e: Exception) {
                    log.error("Error during scheduled cleanup", e)
                }
            }
        }

        vertx.eventBus().consumer<JsonObject>(ADDRESS_TRIGGER_CLEANUP) { message ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val forceAggressive = message.body().getBoolean("forceAggressive", false)
                    val result = performCleanup(forceAggressive)
                    message.reply(JsonObject()
                        .put("success", true)
                        .put("connectionsRemoved", result.first)
                        .put("cacheEntries", result.second)
                    )
                } catch (e: Exception) {
                    log.error("Error during triggered cleanup", e)
                    message.reply(JsonObject().put("success", false).put("error", e.message))
                }
            }
        }

        log.info("CleanupVerticle started successfully")
    }

    /**
     * Performs the cleanup process.
     *
     * @param aggressive If true, cleans up connections even if they might still be in use
     * @return Pair of (removedConnections, remainingCacheEntries)
     */
    private suspend fun performCleanup(aggressive: Boolean = false): Pair<Int, Int> {
        debug.log(DebugType.CLEANUP_STARTING_PROCESS, listOf(aggressive))

        var connectionsRemoved = 0

        try {
            validateCacheIntegrity()
            val connectionsToClear = mutableListOf<String>()

            // Find expired inactive connections
            val expiredConnections = connectionStore.findInactiveConnections(connectionExpiry)

            debug.log(DebugType.CLEANUP_FOUND_INACTIVE_CONNECTIONS, listOf(expiredConnections.size, (connectionExpiry/1000/60)))

            connectionsToClear.addAll(expiredConnections.map { it._id })

            // If aggressive cleanup is enabled, find disconnected non-reconnectable connections
            if (aggressive) {
                val recentlyDisconnected = connectionStore.findInactiveConnections(reconnectTimeout)
                    .filter { !it.reconnectable || it.upSocketId == null }

                debug.log(DebugType.CLEANUP_FOUND_NON_RECONNECTABLE, listOf(recentlyDisconnected.size))

                connectionsToClear.addAll(recentlyDisconnected.map { it._id })
            }

            for (connectionId in connectionsToClear) {
                try {
                    if (cleanupStaleConnection(connectionId)) {
                        connectionsRemoved++
                    }
                } catch (e: Exception) {
                    log.error("Failed to clean up connection $connectionId", e)
                }
            }

            // Check for any orphaned connections in the cache
            val cacheConnectionIds = connectionCache.getAllConnectedIds()
            val orphanedConnections = cacheConnectionIds.filter { connectionId ->
                if (connectionStore.exists(connectionId)) {
                    false
                } else {
                    log.warn("Found orphaned connection in cache: $connectionId")
                    true
                }
            }

            for (connectionId in orphanedConnections) {
                try {
                    log.info("Removing orphaned cache entry: $connectionId")
                    connectionCache.removeUpSocket(connectionId)
                    connectionCache.removeDownSocket(connectionId)
                    connectionCache.removeConnection(connectionId)
                    connectionsRemoved++
                } catch (e: Exception) {
                    log.error("Error removing orphaned cache entry $connectionId", e)
                }
            }

            return Pair(connectionsRemoved, connectionCache.getConnectionCount())

        } catch (e: Exception) {
            log.error("Error during cleanup process", e)
            throw e
        }
    }

    /**
     * Validates that cache and database are in sync
     */
    private suspend fun validateCacheIntegrity() {
        val cacheConnectionIds = connectionCache.getAllConnectedIds()

        debug.log(DebugType.CLEANUP_VALIDATED_CACHE, listOf(cacheConnectionIds.size))
    }

    /**
     * Cleans up a stale connection
     */
    private suspend fun cleanupStaleConnection(connectionId: String): Boolean {
        return try {
            val upSocket = connectionCache.getUpSocket(connectionId)
            val downSocket = connectionCache.getDownSocket(connectionId)

            if (upSocket != null && !upSocket.isClosed) {
                try {
                    upSocket.close()
                } catch (e: Exception) {
                    log.warn("Error closing up socket for $connectionId", e)
                }
            }

            if (downSocket != null && !downSocket.isClosed) {
                try {
                    downSocket.close()
                } catch (e: Exception) {
                    log.warn("Error closing down socket for $connectionId", e)
                }
            }

            val result = vertx.eventBus().request<JsonObject>(
                UpSocketVerticle.ADDRESS_CONNECTION_CLEANUP,
                JsonObject().put("connectionId", connectionId)
            ).await().body().getBoolean("success", false)

            if (!result) {
                // If the regular cleanup fails, try direct removal
                connectionCache.removeUpSocket(connectionId)
                connectionCache.removeDownSocket(connectionId)
                connectionCache.removeConnection(connectionId)
                connectionStore.delete(connectionId)
            }

            true
        } catch (e: Exception) {
            log.error("Failed to clean up connection $connectionId", e)
            false
        }
    }
}