package com.minare.controller

import com.minare.cache.ConnectionCache
import com.minare.core.models.Connection
import com.minare.utils.EntityGraph
import com.minare.worker.CommandSocketVerticle
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import com.minare.core.entity.ReflectionCache
import com.minare.persistence.*
import com.minare.config.InternalInjectorHolder
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Controls connection lifecycle and maintains in-memory connection worker.
 * Provides fast access to connection objects and associated websockets.
 */
@Singleton
open class ConnectionController @Inject constructor(
    val connectionStore: ConnectionStore,
    val connectionCache: ConnectionCache,
    val channelStore: ChannelStore,
    val contextStore: ContextStore,
    val entityStore: EntityStore,
    val reflectionCache: ReflectionCache
) {
    private val log = LoggerFactory.getLogger(ConnectionController::class.java)

    /**
     * Create a new connection and store it in database first, then in memory
     */
    suspend fun createConnection(): Connection {
        // Create in database first (source of truth)
        val connection = connectionStore.create()

        // Then update cache
        connectionCache.storeConnection(connection)
        log.info("Connection created and stored: id={}, commandSocketId={}, updateSocketId={}",
            connection._id, connection.commandSocketId, connection.updateSocketId)
        return connection
    }

    /**
     * Get a connection by ID from memory or database
     */
    suspend fun getConnection(connectionId: String): Connection {
        val cachedConnection = connectionCache.getConnection(connectionId)

        if (cachedConnection != null) {
            log.debug("Connection found in cache: id={}, commandSocketId={}, updateSocketId={}",
                connectionId, cachedConnection.commandSocketId, cachedConnection.updateSocketId)
            return cachedConnection
        }

        // Not in cache, fetch from database (source of truth)
        val connection = connectionStore.find(connectionId)

        // Update cache with fresh data from database
        connectionCache.storeConnection(connection)
        log.debug("Connection loaded from database to cache: id={}, commandSocketId={}, updateSocketId={}",
            connection._id, connection.commandSocketId, connection.updateSocketId)

        return connection
    }

    /**
     * Check if a connection exists in memory
     */
    suspend fun hasConnection(connectionId: String): Boolean {
        return connectionCache.hasConnection(connectionId)
    }

    /**
     * Check if a connection exists in DB and is reconnectable
     */
    suspend fun isConnectionReconnectable(connectionId: String): Boolean {
        try {
            val connection = connectionStore.find(connectionId)

            // Check if active within the reconnection window
            val now = System.currentTimeMillis()
            val reconnectWindow = 30000L // 30 seconds, should match CleanupVerticle.CONNECTION_RECONNECT_WINDOW_MS

            // Return true if reconnectable flag is true and within reconnection window
            return connection.reconnectable && (now - connection.lastActivity < reconnectWindow)
        } catch (e: Exception) {
            log.error("Error checking if connection is reconnectable: {}", connectionId, e)
            return false
        }
    }

    /**
     * Update a connection in database first, then in memory
     */
    suspend fun updateConnection(connection: Connection): Connection {
        // Update database first (source of truth)
        val updatedConnection = connectionStore.updateSocketIds(
            connection._id,
            connection.commandSocketId,
            connection.updateSocketId
        )

        // Then update cache with fresh data from database
        connectionCache.storeConnection(updatedConnection)
        log.debug("Connection updated: id={}, commandSocketId={}, updateSocketId={}",
            updatedConnection._id, updatedConnection.commandSocketId, updatedConnection.updateSocketId)

        return updatedConnection
    }

    /**
     * Register a command WebSocket for a connection
     */
    suspend fun registerCommandSocket(connectionId: String, websocket: ServerWebSocket, socketId: String? = null) {
        try {
            // Close existing socket if any
            connectionCache.getCommandSocket(connectionId)?.let { existingSocket ->
                try {
                    existingSocket.close()
                    log.info("Closed existing command socket for connection {}", connectionId)
                } catch (e: Exception) {
                    log.warn("Failed to close existing command socket for {}", connectionId, e)
                }
            }

            val connection = getConnection(connectionId)
            val updatedConnection = connection.copy(
                commandSocketId = socketId ?: "cs-${java.util.UUID.randomUUID()}",
                lastUpdated = System.currentTimeMillis(),
                lastActivity = System.currentTimeMillis()
            )

            // Update database first (source of truth)
            val persistedConnection = connectionStore.updateSocketIds(
                connectionId,
                updatedConnection.commandSocketId,
                updatedConnection.updateSocketId
            )

            if (persistedConnection == null) {
                throw IllegalStateException("Failed to update command socket ID in database for connection: $connectionId")
            }

            // Then update cache with data from database
            connectionCache.storeConnection(persistedConnection)
            connectionCache.storeCommandSocket(connectionId, websocket, persistedConnection)

            log.info("Command socket registered: connection={}, commandSocketId={}",
                connectionId, persistedConnection.commandSocketId)
        } catch (e: Exception) {
            log.error("Failed to register command WebSocket for {}", connectionId, e)
            throw e
        }
    }

    /**
     * Register an update WebSocket for a connection
     */
    suspend fun registerUpdateSocket(connectionId: String, websocket: ServerWebSocket, socketId: String? = null) {
        try {
            if (connectionCache.getCommandSocket(connectionId) == null) {
                throw IllegalStateException("Cannot register update socket: no command socket exists for $connectionId")
            }

            // Close existing update socket if any
            connectionCache.getUpdateSocket(connectionId)?.let { existingSocket ->
                try {
                    existingSocket.close()
                    log.info("Closed existing update socket for connection {}", connectionId)
                } catch (e: Exception) {
                    log.warn("Failed to close existing update socket for {}", connectionId, e)
                }
            }

            val connection = getConnection(connectionId)
            log.info("Before update socket registration - Connection {}: commandSocketId={}, updateSocketId={}",
                connectionId, connection.commandSocketId, connection.updateSocketId)
            val wasFullyConnected = connection.updateSocketId != null

            val updatedConnection = connection.copy(
                updateSocketId = socketId ?: "us-${java.util.UUID.randomUUID()}",
                lastUpdated = System.currentTimeMillis(),
                lastActivity = System.currentTimeMillis()
            )

            // Update database first (source of truth)
            log.info("Updating socket IDs in DB - Connection {}: commandSocketId={}, updateSocketId={}",
                connectionId, updatedConnection.commandSocketId, updatedConnection.updateSocketId)

            val persistedConnection = connectionStore.updateSocketIds(
                connectionId,
                updatedConnection.commandSocketId,
                updatedConnection.updateSocketId
            )

            if (persistedConnection == null) {
                throw IllegalStateException("Failed to update update socket ID in database for connection: $connectionId")
            }

            // Then update cache with data from database
            connectionCache.storeConnection(persistedConnection)
            connectionCache.storeUpdateSocket(connectionId, websocket, persistedConnection)

            log.info("After DB update - Connection {}: commandSocketId={}, updateSocketId={}",
                connectionId, persistedConnection.commandSocketId, persistedConnection.updateSocketId)

            // Check if the client is now fully connected but wasn't before
            val isNowFullyConnected = persistedConnection.updateSocketId != null
            if (!wasFullyConnected && isNowFullyConnected) {
                // Client has become fully connected
                onClientFullyConnected(persistedConnection)
            }
        } catch (e: Exception) {
            log.error("Failed to register update WebSocket for {}", connectionId, e)
            throw e
        }
    }

    /**
     * Hook called when a client becomes fully connected (has both command and update sockets).
     * Applications can override this to handle connection-specific logic.
     *
     * @param connection The fully connected connection
     */
    open suspend fun onClientFullyConnected(connection: Connection) {
        log.info("Client {} is now fully connected", connection._id)
        // Default implementation is empty - application should override this
    }

    /**
     * Get the CommandSocketVerticle for advanced socket handling
     * This uses the internal injector to access the verticle directly
     */
    fun getCommandSocketVerticle(): CommandSocketVerticle? {
        return try {
            InternalInjectorHolder.getInstance<CommandSocketVerticle>()
        } catch (e: Exception) {
            log.warn("Failed to get CommandSocketVerticle instance: {}", e.message)
            null
        }
    }

    /**
     * Get the command WebSocket for a connection
     */
    fun getCommandSocket(connectionId: String): ServerWebSocket? {
        return connectionCache.getCommandSocket(connectionId)
    }

    /**
     * Get the update WebSocket for a connection
     */
    fun getUpdateSocket(connectionId: String): ServerWebSocket? {
        return connectionCache.getUpdateSocket(connectionId)
    }

    /**
     * Get the connection ID for a command WebSocket
     */
    fun getConnectionIdForCommandSocket(websocket: ServerWebSocket): String? {
        return connectionCache.getConnectionIdForCommandSocket(websocket)
    }

    /**
     * Get the connection ID for an update WebSocket
     */
    fun getConnectionIdForUpdateSocket(websocket: ServerWebSocket): String? {
        return connectionCache.getConnectionIdForUpdateSocket(websocket)
    }

    /**
     * Get the connection for a command WebSocket
     */
    fun getConnectionForCommandSocket(websocket: ServerWebSocket): Connection? {
        return connectionCache.getConnectionForCommandSocket(websocket)
    }

    /**
     * Get the connection for an update WebSocket
     */
    fun getConnectionForUpdateSocket(websocket: ServerWebSocket): Connection? {
        return connectionCache.getConnectionForUpdateSocket(websocket)
    }

    /**
     * Remove a command WebSocket and mark the connection for reconnection instead of immediately removing it
     */
    suspend fun markCommandSocketDisconnected(connectionId: String) {
        try {
            // Close and remove socket from cache
            connectionCache.removeCommandSocket(connectionId)?.let { socket ->
                try {
                    if (!socket.isClosed()) {
                        socket.close()
                    }
                } catch (e: Exception) {
                    log.warn("Error closing command socket for {}", connectionId, e)
                }
            }

            // Update the connection status in the database but don't delete it yet
            // This allows reconnection within the reconnection window
            val updatedConnection = connectionStore.updateLastActivity(connectionId)

            if (updatedConnection == null) {
                log.warn("Failed to update last activity for connection {}, it may already be deleted", connectionId)
                return
            }

            // Update cache with latest from database
            connectionCache.storeConnection(updatedConnection)

            log.info(
                "Command socket for connection {} marked as disconnected, available for reconnection",
                connectionId
            )
        } catch (e: Exception) {
            log.error("Failed to mark command WebSocket disconnected for {}", connectionId, e)
            throw e
        }
    }

    /**
     * Remove a command WebSocket
     * This is a primary operation that will also remove the update socket and connection
     */
    suspend fun removeCommandSocket(connectionId: String) {
        try {
            connectionCache.removeCommandSocket(connectionId)?.let { socket ->
                try {
                    if (!socket.isClosed()) {
                        socket.close()
                    }
                } catch (e: Exception) {
                    log.warn("Error closing command socket for {}", connectionId, e)
                }
            }

            removeUpdateSocket(connectionId)

            try {
                connectionStore.delete(connectionId)
                log.info("Connection {} deleted from database", connectionId)
            } catch (e: Exception) {
                log.error("Failed to delete connection {} from database: {}", connectionId, e.message)
            }
            connectionCache.removeConnection(connectionId)
            log.info("Connection {} removed from cache", connectionId)

        } catch (e: Exception) {
            log.error("Failed to remove command WebSocket for {}", connectionId, e)
            throw e
        }
    }

    /**
     * Remove an update WebSocket.
     * This is a secondary operation that does not affect the command socket or remove the connection.
     */
    suspend fun removeUpdateSocket(connectionId: String) {
        try {
            // Close and remove socket from cache
            connectionCache.removeUpdateSocket(connectionId)?.let { socket ->
                try {
                    if (!socket.isClosed()) {
                        socket.close()
                    }
                } catch (e: Exception) {
                    log.warn("Error closing update socket for {}", connectionId, e)
                }
            }

            // Update database first (source of truth)
            val connection = connectionCache.getConnection(connectionId)
            if (connection != null) {
                val updatedConnection = connection.copy(
                    updateSocketId = null,
                    lastUpdated = System.currentTimeMillis(),
                    lastActivity = System.currentTimeMillis()
                )

                try {
                    val persistedConnection = connectionStore.updateSocketIds(
                        connectionId,
                        updatedConnection.commandSocketId,
                        null
                    )

                    if (persistedConnection == null) {
                        log.warn("Failed to update update socket ID in database for connection: {}", connectionId)
                    } else {
                        // Update cache with database data
                        connectionCache.storeConnection(persistedConnection)
                        log.info("Update socket removed for connection {}", connectionId)
                    }
                } catch (e: Exception) {
                    // This might fail if connection was already deleted or is being deleted concurrently
                    log.warn("Failed to update database for update socket removal: {}", e.message)
                    // Update cache anyway to maintain consistency with what we tried to do
                    connectionCache.storeConnection(updatedConnection)
                }
            }
        } catch (e: Exception) {
            log.error("Failed to remove update WebSocket for {}", connectionId, e)
            // Don't rethrow - we want this to be resilient to failures
        }
    }

    /**
     * Handle the closure of a command socket by marking it for reconnection instead of cleaning up immediately
     */
    suspend fun handleCommandSocketClosed(connectionId: String) {
        try {
            log.info("Command socket closed for connection {}, marking for potential reconnection", connectionId)

            // Mark the connection as reconnectable in database (source of truth)
            val updatedConnection = connectionStore.updateReconnectable(connectionId, true)

            if (updatedConnection == null) {
                log.warn("Failed to mark connection {} as reconnectable, it may already be deleted", connectionId)
            } else {
                // Update cache with database data
                connectionCache.storeConnection(updatedConnection)
            }

            // Remove socket from cache
            connectionCache.removeCommandSocket(connectionId)

            // We don't remove the connection from channels yet - that happens on final cleanup
            // if reconnection doesn't happen
        } catch (e: Exception) {
            log.error("Error handling command socket closure for {}", connectionId, e)
        }
    }

    /**
     * Get all connected client IDs (those with at least a command socket)
     */
    fun getAllConnectedIds(): List<String> {
        return connectionCache.getAllConnectedIds()
    }

    /**
     * Get count of active connections (those with at least a command socket)
     */
    fun getConnectionCount(): Int {
        return connectionCache.getConnectionCount()
    }

    /**
     * Get count of connections with both sockets established
     */
    fun getFullyConnectedCount(): Int {
        return connectionCache.getFullyConnectedCount()
    }

    /**
     * Check if a connection has both sockets established
     */
    fun isFullyConnected(connectionId: String): Boolean {
        return connectionCache.isFullyConnected(connectionId)
    }

    /**
     * Synchronize a connection with all entities in its channels
     * This is typically called when a connection is first established
     *
     * @param connectionId The ID of the connection to synchronize
     * @return True if sync was successful, false otherwise
     */
    suspend fun syncConnection(connectionId: String): Boolean {
        try {
            log.info("Starting sync for connection: {}", connectionId)

            val channels = channelStore.getChannelsForClient(connectionId)

            if (channels.isEmpty()) {
                log.info("No channels found for connection: {}", connectionId)
                return true
            }

            for (channelId in channels) {
                syncChannelToConnection(channelId, connectionId)
            }

            log.info("Sync completed for connection: {}", connectionId)
            return true

        } catch (e: Exception) {
            log.error("Error during connection sync: {}", connectionId, e)
            return false
        }
    }

    /**
     * Synchronize all entities in a channel to a specific connection
     *
     * @param channelId The channel containing the entities
     * @param connectionId The connection to sync to
     */
    suspend fun syncChannelToConnection(channelId: String, connectionId: String) {
        try {
            log.debug("Syncing channel {} to connection {}", channelId, connectionId)
            val entityIds = contextStore.getEntityIdsByChannel(channelId)

            if (entityIds.isEmpty()) {
                log.debug("No entities found in channel: {}", channelId)
                return
            }

            val documentGraph = entityStore.buildDocumentGraph(entityIds)

            // Convert the document graph to JSON format suitable for client consumption
            val syncData = EntityGraph.documentGraphToJson(documentGraph)

            syncData.put("channelId", channelId)
            syncData.put("timestamp", System.currentTimeMillis())

            val syncMessage = JsonObject()
                .put("type", "sync")
                .put("data", syncData)

            // Send to the client
            val commandSocket = getCommandSocket(connectionId)
            if (commandSocket != null && !commandSocket.isClosed()) {
                commandSocket.writeTextMessage(syncMessage.encode())
                log.debug("Sent sync data for channel {} to connection {}", channelId, connectionId)
            } else {
                log.warn("Cannot sync: command socket not found or closed for connection {}", connectionId)
            }

        } catch (e: Exception) {
            log.error("Error syncing channel {} to connection {}", channelId, connectionId, e)
        }
    }

    /**
     * Handles the cleanup when a connection is terminated.
     * Channel cleanup is decoupled from connection existence.
     */
    suspend fun cleanupConnection(connectionId: String) {
        log.info("Cleaning up connection {}", connectionId)

        try {
            val removedCount = channelStore.removeClientFromAllChannels(connectionId)
            log.info("Removed connection {} from {} channels", connectionId, removedCount)
        } catch (e: Exception) {
            log.error("Error removing connection {} from channels: {}", connectionId, e.message)
        }

        // Clean up sockets and cache
        try {
            connectionCache.getCommandSocket(connectionId)?.let { socket ->
                try {
                    if (!socket.isClosed()) socket.close()
                } catch (e: Exception) {
                    log.warn("Error closing command socket for {}", connectionId, e)
                }
            }

            connectionCache.getUpdateSocket(connectionId)?.let { socket ->
                try {
                    if (!socket.isClosed()) socket.close()
                } catch (e: Exception) {
                    log.warn("Error closing update socket for {}", connectionId, e)
                }
            }

            connectionCache.removeCommandSocket(connectionId)
            connectionCache.removeUpdateSocket(connectionId)
            connectionCache.removeConnection(connectionId)

            log.info("Connection {} removed from cache", connectionId)
        } catch (e: Exception) {
            log.error("Error cleaning up connection {} cache entries: {}", connectionId, e.message)
        }

        // Step 3: Finally try to delete the connection from the database
        // This is last because it's the least important - if the connection
        // is already gone, that's actually fine
        try {
            connectionStore.delete(connectionId)
            log.info("Connection {} deleted from database", connectionId)
        } catch (e: Exception) {
            log.debug(
                "Could not delete connection {} from database - it may already be deleted: {}",
                connectionId, e.message
            )
            // This is expected in some cases and not an error
        }

        log.info("Connection {} cleanup completed", connectionId)
    }
}