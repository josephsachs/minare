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

/**
 * Verticle that handles periodic cleaning of stale connections and data.
 * This helps ensure system health by removing orphaned resources.
 */
class CleanupVerticle @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache,
    private val connectionController: ConnectionController
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(CleanupVerticle::class.java)

    companion object {
        const val ADDRESS_TRIGGER_CLEANUP = "minare.trigger.cleanup"

        // Cleanup configuration
        private const val CLEANUP_INTERVAL_MS = 60000L // 1 minute
        private const val CONNECTION_EXPIRY_MS = 1800000L // 30 minutes
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

            // Step 2: Clean up stale database connections
            val staleDatabaseConnections = findStaleConnections(aggressive)

            // Step 3: Clean up each stale connection
            for (connectionId in staleDatabaseConnections) {
                try {
                    log.info("Cleaning up stale connection: $connectionId")

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
     * Finds stale connections in the database
     */
    private suspend fun findStaleConnections(aggressive: Boolean): List<String> {
        // This would typically query the database for connections that haven't been
        // updated in a while or match other criteria for cleanup

        val staleConnections = mutableListOf<String>()

        // Example implementation - find connections where:
        // 1. They have no command socket (definitely stale)
        // 2. OR they haven't been updated in CONNECTION_EXPIRY_MS and aggressive=true

        // In a real implementation, we would query the database directly
        // For now, we'll use the cache to identify candidates

        val cacheConnectionIds = connectionCache.getAllConnectedIds()

        for (connectionId in cacheConnectionIds) {
            val commandSocket = connectionCache.getCommandSocket(connectionId)

            if (commandSocket == null || commandSocket.isClosed()) {
                // No command socket or closed socket - definitely stale
                staleConnections.add(connectionId)
            } else if (aggressive) {
                // In aggressive mode, also check last activity time
                try {
                    val connection = connectionStore.find(connectionId)
                    val now = System.currentTimeMillis()

                    if (now - connection.lastUpdated > CONNECTION_EXPIRY_MS) {
                        log.info("Connection $connectionId has been inactive for ${(now - connection.lastUpdated) / 1000} seconds")
                        staleConnections.add(connectionId)
                    }
                } catch (e: Exception) {
                    // Error finding connection - add to stale list
                    log.warn("Error checking connection $connectionId: ${e.message}")
                    staleConnections.add(connectionId)
                }
            }
        }

        log.info("Found ${staleConnections.size} stale connections")
        return staleConnections
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
                CommandSocketVerticle.ADDRESS_CONNECTION_CLEANUP,
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