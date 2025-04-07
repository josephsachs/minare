package com.minare.worker

import com.minare.cache.ConnectionCache
import com.minare.controller.ConnectionController
import com.minare.persistence.ConnectionStore
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject
import com.minare.worker.command.CommandVerticle

/**
 * Verticle that handles periodic cleaning of stale connections and data.
 * This helps ensure system health by removing orphaned resources.
 */
class CleanupVerticle @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(CleanupVerticle::class.java)

    companion object {
        const val ADDRESS_TRIGGER_CLEANUP = "minare.trigger.cleanup"


        private const val CLEANUP_INTERVAL_MS = 60000L

        // Connection reconnection window - how long to keep connections available for reconnect after disconnect
        const val CONNECTION_RECONNECT_WINDOW_MS = 60000L

        // Connection expiry - when to clean up inactive connections
        private const val CONNECTION_EXPIRY_MS = 180000L
    }

    override suspend fun start() {
        log.info("Starting CleanupVerticle")

        // Setup periodic cleanup timer
        vertx.setPeriodic(CLEANUP_INTERVAL_MS) { _ ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    performCleanup()
                } catch (e: Exception) {
                    log.error("Error during scheduled cleanup", e)
                }
            }
        }

        // Setup event bus listener for triggered cleanups
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
        log.info("Starting connection cleanup process (aggressive=${aggressive})")

        // Track metrics
        var connectionsRemoved = 0

        try {
            // Step 1: Verify cache-database consistency
            validateCacheIntegrity()

            // Step 2: Find connections to clean up
            val connectionsToClear = mutableListOf<String>()

            // Find expired inactive connections
            val expiredConnections = connectionStore.findInactiveConnections(CONNECTION_EXPIRY_MS)
            log.info("Found ${expiredConnections.size} expired connections (inactive > ${CONNECTION_EXPIRY_MS/1000/60} minutes)")
            connectionsToClear.addAll(expiredConnections.map { it._id })

            // Find disconnected non-reconnectable connections
            if (aggressive) {
                // Only if aggressive cleanup is enabled
                val recentlyDisconnected = connectionStore.findInactiveConnections(CONNECTION_RECONNECT_WINDOW_MS)
                    .filter { !it.reconnectable || it.commandSocketId == null }

                log.info("Found ${recentlyDisconnected.size} recently disconnected non-reconnectable connections")
                connectionsToClear.addAll(recentlyDisconnected.map { it._id })
            }

            // Step 3: Clean up each connection
            for (connectionId in connectionsToClear) {
                try {
                    log.info("Cleaning up connection: $connectionId")

                    // Attempt to use the standard cleanup process
                    val cleanupSuccess = cleanupStaleConnection(connectionId)

                    if (cleanupSuccess) {
                        connectionsRemoved++
                    }
                } catch (e: Exception) {
                    log.error("Error cleaning up connection $connectionId", e)
                }
            }

            // Step 4: Check for any orphaned connections in the cache
            val cacheConnectionIds = connectionCache.getAllConnectedIds()
            val orphanedConnections = cacheConnectionIds.filter { connectionId ->
                try {
                    // Attempt to find this connection in the database
                    connectionStore.find(connectionId)
                    false // Found in database, not orphaned
                } catch (e: Exception) {
                    // Not found in database, orphaned in cache
                    log.warn("Found orphaned connection in cache: $connectionId")
                    true
                }
            }

            // Clean up orphaned cache entries
            for (connectionId in orphanedConnections) {
                try {
                    log.info("Removing orphaned cache entry: $connectionId")
                    // Clean up from cache
                    connectionCache.removeCommandSocket(connectionId)
                    connectionCache.removeUpdateSocket(connectionId)
                    connectionCache.removeConnection(connectionId)
                    connectionsRemoved++
                } catch (e: Exception) {
                    log.error("Error removing orphaned cache entry $connectionId", e)
                }
            }

            // Return metrics
            return Pair(connectionsRemoved, connectionCache.getConnectionCount())

        } catch (e: Exception) {
            log.error("Error during cleanup process", e)
            throw e
        }
    }

    /**
     * Validates the integrity of the cache against the database
     */
    private suspend fun validateCacheIntegrity() {
        log.debug("Validating cache integrity")

        val cacheConnectionIds = connectionCache.getAllConnectedIds()
        log.debug("Found ${cacheConnectionIds.size} connections in cache")

        // We could add more validation here if needed
    }

    /**
     * Cleans up a stale connection
     */
    private suspend fun cleanupStaleConnection(connectionId: String): Boolean {
        return try {
            // First try closing any sockets
            val commandSocket = connectionCache.getCommandSocket(connectionId)
            val updateSocket = connectionCache.getUpdateSocket(connectionId)

            if (commandSocket != null && !commandSocket.isClosed()) {
                try {
                    commandSocket.close()
                } catch (e: Exception) {
                    log.warn("Error closing command socket for $connectionId", e)
                }
            }

            if (updateSocket != null && !updateSocket.isClosed()) {
                try {
                    updateSocket.close()
                } catch (e: Exception) {
                    log.warn("Error closing update socket for $connectionId", e)
                }
            }

            // Then use the controller to clean up
            val result = vertx.eventBus().request<JsonObject>(
                CommandVerticle.ADDRESS_CONNECTION_CLEANUP,
                JsonObject().put("connectionId", connectionId)
            ).await().body().getBoolean("success", false)

            if (!result) {
                // If the regular cleanup fails, try direct removal
                connectionCache.removeCommandSocket(connectionId)
                connectionCache.removeUpdateSocket(connectionId)
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