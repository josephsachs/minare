package com.minare.controller

import com.minare.application.config.FrameworkConfig
import com.minare.cache.ConnectionCache
import com.minare.core.transport.models.Connection
import com.minare.core.transport.upsocket.UpSocketVerticle
import io.vertx.core.http.ServerWebSocket
import org.slf4j.LoggerFactory
import com.minare.core.storage.interfaces.*
import com.minare.core.config.InternalInjectorHolder
import com.minare.core.transport.CleanupVerticle
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType as DebugType
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Controls connection lifecycle and maintains in-memory connection worker.
 * Provides fast access to connection objects and associated websockets.
 */
@Singleton
open class ConnectionController @Inject constructor() {
    @Inject private lateinit var frameworkConfig: FrameworkConfig
    @Inject private lateinit var connectionStore: ConnectionStore
    @Inject private lateinit var connectionCache: ConnectionCache
    @Inject private lateinit var channelStore: ChannelStore
    @Inject private lateinit var debug: DebugLogger

    private val log = LoggerFactory.getLogger(ConnectionController::class.java)

    /**
     * Create a new connection and store it in database first, then in memory
     */
    suspend fun createConnection(): Connection {
        val connection = connectionStore.create()

        connectionCache.storeConnection(connection)

        debug.log(DebugType.CONNECTION_CONTROLLER_CREATE_CONNECTION, listOf(connection._id, connection.upSocketId, connection.downSocketId))

        return connection
    }

    /**
     * Get a connection by ID from memory or database
     */
    suspend fun getConnection(connectionId: String): Connection {
        val cachedConnection = connectionCache.getConnection(connectionId)

        if (cachedConnection != null) {
            debug.log(DebugType.CONNECTION_CONTROLLER_FOUND_CONNECTION, listOf(
                connectionId,
                cachedConnection.upSocketId.orEmpty(),
                cachedConnection.downSocketId.orEmpty()
            ))

            return cachedConnection
        }

        val connection = connectionStore.find(connectionId)

        if (true in listOf(connection.upSocketId?.isBlank(), connection.downSocketId?.isBlank())) {
            log.warn("Connection controller found incomplete connection for $connectionId,\nConsider tagging for deletion")

            // TODO: Send a connection re-establish message to the transport verticles
            return connection
        }

        connectionCache.storeConnection(connection)

        debug.log(DebugType.CONNECTION_CONTROLLER_STORED_CONNECTION, listOf(
            connectionId,
            connection.upSocketId.orEmpty(),
            connection.downSocketId.orEmpty()
        ))

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

            val now = System.currentTimeMillis()
            val reconnectWindow = frameworkConfig.sockets.connection.reconnectTimeout

            return connection.reconnectable &&
                    (now - connection.lastActivity < reconnectWindow)
        } catch (e: Exception) {
            log.error("Error checking if connection is reconnectable: {}", connectionId, e)

            return false
        }
    }

    /**
     * Update a connection in database first, then in memory
     */
    suspend fun storeTransportSockets(connection: Connection): Connection {
        if (true in listOf(connection.upSocketId?.isBlank(), connection.downSocketId?.isBlank())) {
            log.error("Connection controller tried storing incomplete transport sockets profiles on connection ${connection._id}")

            return connection
        }

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

        debug.log(DebugType.CONNECTION_CONTROLLER_UPDATE_SOCKETS, listOf(connection._id, connection.downSocketId, connection.downSocketDeploymentId))

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

            debug.log(DebugType.CONNECTION_CONTROLLER_UPSOCKET_DISCONNECT, listOf(connectionId))
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

                debug.log(DebugType.CONNECTION_CONTROLLER_CONNECTION_DELETED, listOf(connectionId))
            } catch (e: Exception) {
                log.error("Failed to delete connection {} from database: {}", connectionId, e.message)
            }

            connectionCache.removeConnection(connectionId)

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
                    debug.log(DebugType.CONNECTION_CONTROLLER_REMOVE_DOWNSOCKET, listOf(connectionId))

                } catch (e: Exception) {
                    // This might fail if connection was already deleted or is being deleted concurrently
                    log.warn("Failed to update database for down socket removal: {}", e.message)
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
            debug.log(DebugType.CONNECTION_CONTROLLER_UPSOCKET_CLOSED, listOf(connectionId))

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
        try {
            val removedCount = channelStore.removeClientFromAllChannels(connectionId)

            debug.log(DebugType.CONNECTION_CONTROLLER_CLEANUP_CONNECTION, listOf(connectionId, removedCount))
        } catch (e: Exception) {
            log.error("Error removing connection {} from channels: {}", connectionId, e.message)
        }

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
        } catch (e: Exception) {
            log.error("Error cleaning up connection {} cache entries: {}", connectionId, e.message)
        }

        try {
            connectionStore.delete(connectionId)
        } catch (e: Exception) {
            debug.log(DebugType.CONNECTION_CONTROLLER_ALREADY_DELETED_WARNING, listOf(connectionId, e.message.toString()))
        }
    }
}