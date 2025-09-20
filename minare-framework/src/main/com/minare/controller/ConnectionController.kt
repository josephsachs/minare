package com.minare.controller

import com.minare.cache.ConnectionCache
import com.minare.core.transport.models.Connection
import com.minare.worker.upsocket.UpSocketVerticle
import io.vertx.core.http.ServerWebSocket
import org.slf4j.LoggerFactory
import com.minare.core.storage.interfaces.*
import com.minare.core.config.InternalInjectorHolder
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Controls connection lifecycle and maintains in-memory connection worker.
 * Provides fast access to connection objects and associated websockets.
 */
@Singleton
open class ConnectionController @Inject constructor() {
    @Inject private lateinit var connectionStore: ConnectionStore
    @Inject private lateinit var connectionCache: ConnectionCache
    @Inject private lateinit var channelStore: ChannelStore

    private val log = LoggerFactory.getLogger(ConnectionController::class.java)

    /**
     * Create a new connection and store it in database first, then in memory
     */
    suspend fun createConnection(): Connection {
        val connection = connectionStore.create()

        connectionCache.storeConnection(connection)
        log.info("Connection created and stored: id={}, upSocketId={}, downSocketId={}",
            connection._id, connection.upSocketId, connection.downSocketId)
        return connection
    }

    /**
     * Get a connection by ID from memory or database
     */
    suspend fun getConnection(connectionId: String): Connection {
        val cachedConnection = connectionCache.getConnection(connectionId)

        if (cachedConnection != null) {
            log.debug("Connection found in cache: id={}, upSocketId={}, downSocketId={}",
                connectionId, cachedConnection.upSocketId, cachedConnection.downSocketId)
            return cachedConnection
        }

        val connection = connectionStore.find(connectionId)

        connectionCache.storeConnection(connection)
        log.debug("Connection loaded from database to cache: id={}, upSocketId={}, downSocketId={}",
            connection._id, connection.upSocketId, connection.downSocketId)

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
        connectionCache.storeConnection(
            connectionStore.putUpSocket(
                connection._id,
                connection.upSocketId,
                connection.upSocketDeploymentId
            )
        )

        connectionCache.storeConnection(
            connectionStore.putDownSocket(
                connection._id,
                connection.downSocketId,
                connection.downSocketDeploymentId
            )
        )
        log.debug("Connection updated: id={}, upSocketId={}, downSocketId={}", connection._id, connection.downSocketId, connection.downSocketDeploymentId)

        return connection
    }

    /**
     * Hook called when a client becomes fully connected (has both up and down sockets).
     * Applications can override this to handle connection-specific logic.
     *
     * @param connection The fully connected connection
     */
    open suspend fun onClientFullyConnected(connection: Connection) {
        log.info("Client {} is now fully connected", connection._id)
    }

    /**
     * Get the UpSocketVerticle for advanced socket handling
     * This uses the internal injector to access the verticle directly
     */
    fun getUpSocketVerticle(): UpSocketVerticle? {
        return try {
            InternalInjectorHolder.getInstance<UpSocketVerticle>()
        } catch (e: Exception) {
            log.warn("Failed to get UpSocketVerticle instance: {}", e.message)
            null
        }
    }

    /**
     * Get the up WebSocket for a connection
     */
    fun getUpSocket(connectionId: String): ServerWebSocket? {
        return connectionCache.getUpSocket(connectionId)
    }

    /**
     * Get the update WebSocket for a connection
     */
    fun getUpdateSocket(connectionId: String): ServerWebSocket? {
        return connectionCache.getDownSocket(connectionId)
    }

    /**
     * Get the connection ID for an up WebSocket
     */
    fun getConnectionIdForUpSocket(websocket: ServerWebSocket): String? {
        return connectionCache.getConnectionIdForUpSocket(websocket)
    }

    /**
     * Get the connection ID for an update WebSocket
     */
    fun getConnectionIdForUpdateSocket(websocket: ServerWebSocket): String? {
        return connectionCache.getConnectionIdForDownSocket(websocket)
    }

    /**
     * Get the connection for an up WebSocket
     */
    fun getConnectionForUpSocket(websocket: ServerWebSocket): Connection? {
        return connectionCache.getConnectionForUpSocket(websocket)
    }

    /**
     * Get the connection for an update WebSocket
     */
    fun getConnectionForUpdateSocket(websocket: ServerWebSocket): Connection? {
        return connectionCache.getConnectionForDownSocket(websocket)
    }

    /**
     * Remove an up WebSocket and mark the connection for reconnection instead of immediately removing it
     */
    suspend fun markUpSocketDisconnected(connectionId: String) {
        try {
            connectionCache.removeUpSocket(connectionId)?.let { socket ->
                try {
                    if (!socket.isClosed()) {
                        socket.close()
                    }
                } catch (e: Exception) {
                    log.warn("Error closing up socket for {}", connectionId, e)
                }
            }

            val updatedConnection = connectionStore.updateLastActivity(connectionId)

            if (updatedConnection == null) {
                log.warn("Failed to update last activity for connection {}, it may already be deleted", connectionId)
                return
            }

            connectionCache.storeConnection(updatedConnection)

            log.info("Up socket for connection {} marked as disconnected, available for reconnection", connectionId)
        } catch (e: Exception) {
            log.error("Failed to mark up WebSocket disconnected for {}", connectionId, e)
            throw e
        }
    }

    /**
     * Remove an up WebSocket
     * This is a primary operation that will also remove the down socket and connection
     */
    suspend fun removeUpSocket(connectionId: String) {
        try {
            connectionCache.removeUpSocket(connectionId)?.let { socket ->
                try {
                    if (!socket.isClosed()) {
                        socket.close()
                    }
                } catch (e: Exception) {
                    log.warn("Error closing up socket for {}", connectionId, e)
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
            log.error("Failed to remove up WebSocket for {}", connectionId, e)
            throw e
        }
    }

    /**
     * Remove an update WebSocket.
     * This is a secondary operation that does not affect the up socket or remove the connection.
     */
    suspend fun removeUpdateSocket(connectionId: String) {
        try {
            connectionCache.removeDownSocket(connectionId)?.let { socket ->
                try {
                    if (!socket.isClosed()) {
                        socket.close()
                    }
                } catch (e: Exception) {
                    log.warn("Error closing down socket for {}", connectionId, e)
                }
            }

            val connection = connectionCache.getConnection(connectionId)
            if (connection != null) {
                val updatedConnection = connection.copy(
                    downSocketId = null,
                    downSocketDeploymentId = null,
                    lastUpdated = System.currentTimeMillis(),
                    lastActivity = System.currentTimeMillis()
                )

                try {
                    val persistedConnection = connectionStore.putUpSocket(
                        connectionId,
                        updatedConnection.upSocketId,
                        updatedConnection.upSocketDeploymentId
                    )

                    connectionCache.storeConnection(persistedConnection)
                    log.info("Down socket removed for connection {}", connectionId)

                } catch (e: Exception) {
                    // This might fail if connection was already deleted or is being deleted concurrently
                    log.warn("Failed to update database for down socket removal: {}", e.message)
                    // Update cache anyway to maintain consistency with what we tried to do
                    connectionCache.storeConnection(updatedConnection)
                }
            }
        } catch (e: Exception) {
            log.error("Failed to remove update WebSocket for {}", connectionId, e)
            // Don't rethrow
        }
    }

    /**
     * Handle the closure of an up socket by marking it for reconnection instead of cleaning up immediately
     */
    suspend fun handleUpSocketClosed(connectionId: String) {
        try {
            log.info("Up socket closed for connection {}, marking for potential reconnection", connectionId)

            val updatedConnection = connectionStore.updateReconnectable(connectionId, true)

            if (updatedConnection == null) {
                log.warn("Failed to mark connection {} as reconnectable, it may already be deleted", connectionId)
            } else {
                connectionCache.storeConnection(updatedConnection)
            }

            connectionCache.removeUpSocket(connectionId)

            // Connection will be removed by CleanupVerticle after TTL
        } catch (e: Exception) {
            log.error("Error handling up socket closure for {}", connectionId, e)
        }
    }

    /**
     * Get all connected client IDs (those with at least an up socket)
     */
    fun getAllConnectedIds(): List<String> {
        return connectionCache.getAllConnectedIds()
    }

    /**
     * Get count of active connections (those with at least an up socket)
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
            connectionCache.getUpSocket(connectionId)?.let { socket ->
                try {
                    if (!socket.isClosed()) socket.close()
                } catch (e: Exception) {
                    log.warn("Error closing up socket for {}", connectionId, e)
                }
            }

            connectionCache.getDownSocket(connectionId)?.let { socket ->
                try {
                    if (!socket.isClosed()) socket.close()
                } catch (e: Exception) {
                    log.warn("Error closing down socket for {}", connectionId, e)
                }
            }

            connectionCache.removeUpSocket(connectionId)
            connectionCache.removeDownSocket(connectionId)
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