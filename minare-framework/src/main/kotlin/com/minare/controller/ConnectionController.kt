package com.minare.controller

import com.minare.cache.ConnectionCache
import com.minare.core.models.Connection
import com.minare.persistence.ConnectionStore
import com.minare.persistence.ChannelStore
import com.minare.persistence.ContextStore
import com.minare.persistence.EntityStore
import com.minare.utils.EntityGraph
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Controls connection lifecycle and maintains in-memory connection state.
 * Provides fast access to connection objects and associated websockets.
 */
@Singleton
class ConnectionController @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache,
    private val channelStore: ChannelStore,
    private val contextStore: ContextStore,
    private val entityStore: EntityStore
) {
    private val log = LoggerFactory.getLogger(ConnectionController::class.java)

    /**
     * Create a new connection and store it both in memory and database
     */
    suspend fun createConnection(): Connection {
        val connection = connectionStore.create()
        // Store in memory cache
        connectionCache.storeConnection(connection)
        log.info("Connection created and stored in memory: {}", connection._id)
        return connection
    }

    /**
     * Get a connection by ID from memory or database
     */
    suspend fun getConnection(connectionId: String): Connection {
        // Try memory cache first
        val cachedConnection = connectionCache.getConnection(connectionId)
        if (cachedConnection != null) {
            return cachedConnection
        }

        // Fall back to database
        val connection = connectionStore.find(connectionId)
        // Store in memory cache for future use
        connectionCache.storeConnection(connection)
        log.debug("Connection loaded from database to memory: {}", connection._id)
        return connection
    }

    /**
     * Check if a connection exists in memory
     */
    fun hasConnection(connectionId: String): Boolean {
        return connectionCache.hasConnection(connectionId)
    }

    /**
     * Update a connection in both memory and database
     */
    suspend fun updateConnection(connection: Connection): Connection {
        // Update in memory cache
        connectionCache.storeConnection(connection)

        // Update command socket ID and update socket ID in database
        return connectionStore.updateSocketIds(
            connection._id,
            connection.commandSocketId,
            connection.updateSocketId
        )
    }

    /**
     * Register a command WebSocket for a connection
     */
    suspend fun registerCommandSocket(connectionId: String, websocket: ServerWebSocket, socketId: String? = null) {
        try {
            // Close any existing socket
            connectionCache.getCommandSocket(connectionId)?.let { existingSocket ->
                try {
                    existingSocket.close()
                } catch (e: Exception) {
                    log.warn("Failed to close existing command socket for {}", connectionId, e)
                }
            }

            // Update the connection in memory
            val connection = getConnection(connectionId)
            val updatedConnection = connection.copy(
                commandSocketId = socketId ?: "cs-${java.util.UUID.randomUUID()}",
                lastUpdated = System.currentTimeMillis()
            )

            // Store in cache
            connectionCache.storeConnection(updatedConnection)

            // Register the new socket in cache
            connectionCache.storeCommandSocket(connectionId, websocket, updatedConnection)

            // Update in database
            connectionStore.updateSocketIds(
                connectionId,
                updatedConnection.commandSocketId,
                updatedConnection.updateSocketId
            )
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
            // Verify command socket exists (update socket is secondary)
            if (connectionCache.getCommandSocket(connectionId) == null) {
                throw IllegalStateException("Cannot register update socket: no command socket exists for $connectionId")
            }

            // Close any existing socket
            connectionCache.getUpdateSocket(connectionId)?.let { existingSocket ->
                try {
                    existingSocket.close()
                } catch (e: Exception) {
                    log.warn("Failed to close existing update socket for {}", connectionId, e)
                }
            }

            // Update the connection in memory
            val connection = getConnection(connectionId)
            val updatedConnection = connection.copy(
                updateSocketId = socketId ?: "us-${java.util.UUID.randomUUID()}",
                lastUpdated = System.currentTimeMillis()
            )

            // Store in cache
            connectionCache.storeConnection(updatedConnection)

            // Register the new socket in cache
            connectionCache.storeUpdateSocket(connectionId, websocket, updatedConnection)

            // Update in database
            connectionStore.updateSocketIds(
                connectionId,
                updatedConnection.commandSocketId,
                updatedConnection.updateSocketId
            )
        } catch (e: Exception) {
            log.error("Failed to register update WebSocket for {}", connectionId, e)
            throw e
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
     * Remove a command WebSocket
     * This is a primary operation that will also remove the update socket and connection
     */
    suspend fun removeCommandSocket(connectionId: String) {
        try {
            // Remove from memory cache and close socket
            connectionCache.removeCommandSocket(connectionId)?.let { socket ->
                try {
                    socket.close()
                } catch (e: Exception) {
                    log.warn("Error closing command socket for {}", connectionId, e)
                }
            }

            // Connection should be removed from DB when command socket closes
            // But first we need to clean up the update socket
            removeUpdateSocket(connectionId)

            // Remove from memory cache
            connectionCache.removeConnection(connectionId)
            // Remove from database
            connectionStore.delete(connectionId)
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
            // Remove from memory cache and close socket
            connectionCache.removeUpdateSocket(connectionId)?.let { socket ->
                try {
                    socket.close()
                } catch (e: Exception) {
                    log.warn("Error closing update socket for {}", connectionId, e)
                }
            }

            // Update the connection in memory
            val connection = getConnection(connectionId)
            val updatedConnection = connection.copy(
                updateSocketId = null,
                lastUpdated = System.currentTimeMillis()
            )
            connectionCache.storeConnection(updatedConnection)

            // Update in database
            connectionStore.updateSocketIds(
                connectionId,
                updatedConnection.commandSocketId,
                null
            )
        } catch (e: Exception) {
            log.error("Failed to remove update WebSocket for {}", connectionId, e)
            throw e
        }
    }

    /**
     * Handle the closure of a command socket by cleaning up both sockets and connection.
     * This is called when the primary socket is closed, indicating the connection
     * should be terminated completely.
     */
    suspend fun handleCommandSocketClosed(connectionId: String) {
        // Store update socket (if any) before removing everything
        val updateSocket = connectionCache.getUpdateSocket(connectionId)

        // First remove command socket and its references
        removeCommandSocket(connectionId)

        // If we had an update socket, make sure it's closed too
        updateSocket?.let {
            if (!it.isClosed()) {
                try {
                    it.close()
                } catch (e: Exception) {
                    log.warn("Error closing update socket for connection {}", connectionId, e)
                }
            }
        }

        log.info("Connection {} fully closed and removed", connectionId)
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

            // Get all channels this connection is subscribed to
            val channels = channelStore.getChannelsForClient(connectionId)

            if (channels.isEmpty()) {
                log.info("No channels found for connection: {}", connectionId)
                return true // Nothing to sync
            }

            // For each channel, sync all entities
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
    private suspend fun syncChannelToConnection(channelId: String, connectionId: String) {
        try {
            log.debug("Syncing channel {} to connection {}", channelId, connectionId)

            // Get all entity IDs in this channel
            val entityIds = contextStore.getEntityIdsByChannel(channelId)

            if (entityIds.isEmpty()) {
                log.debug("No entities found in channel: {}", channelId)
                return
            }

            // Option 1: Fetch entities as a map (simpler)
            val entities = entityStore.findEntitiesByIds(entityIds)
            val syncData = EntityGraph.entitiesToJson(entities)

            // Option 2: Build and convert a full graph (with relationships)
            // Use this if relationships between entities are important
            // val graph = entityStore.buildEntityGraph(entityIds)
            // val syncData = EntityGraph.graphToJson(graph)

            // Add metadata
            syncData.put("channelId", channelId)
            syncData.put("timestamp", System.currentTimeMillis())

            // Create the sync message
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
}