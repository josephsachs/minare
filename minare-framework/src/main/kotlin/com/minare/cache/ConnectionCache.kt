package com.minare.cache

import com.minare.core.models.Connection
import io.vertx.core.http.ServerWebSocket

/**
 * Interface for caching connection data and websocket associations.
 * Provides fast access to connections and related websockets.
 */
interface ConnectionCache {
    /**
     * Store a connection in the cache
     */
    fun storeConnection(connection: Connection)

    /**
     * Remove a connection from the cache
     */
    fun removeConnection(connectionId: String)

    /**
     * Get a connection from the cache
     * @return The connection or null if not found
     */
    suspend fun getConnection(connectionId: String): Connection?

    /**
     * Check if a connection exists in the cache
     */
    suspend fun hasConnection(connectionId: String): Boolean

    /**
     * Associate a command socket with a connection
     * @param connectionId The connection ID
     * @param socket The WebSocket instance
     * @param connection The full Connection object
     */
    fun storeCommandSocket(connectionId: String, socket: ServerWebSocket, connection: Connection)

    /**
     * Associate an update socket with a connection
     * @param connectionId The connection ID
     * @param socket The WebSocket instance
     * @param connection The full Connection object
     */
    fun storeUpdateSocket(connectionId: String, socket: ServerWebSocket, connection: Connection)

    /**
     * Get the command socket for a connection
     */
    fun getCommandSocket(connectionId: String): ServerWebSocket?

    /**
     * Get the update socket for a connection
     */
    fun getUpdateSocket(connectionId: String): ServerWebSocket?

    /**
     * Get the connection ID associated with a command socket
     * @return The connection ID or null if not found
     */
    fun getConnectionIdForCommandSocket(socket: ServerWebSocket): String?

    /**
     * Get the connection ID associated with an update socket
     * @return The connection ID or null if not found
     */
    fun getConnectionIdForUpdateSocket(socket: ServerWebSocket): String?

    /**
     * Get the connection associated with a command socket
     * @return The full Connection object or null if not found
     */
    fun getConnectionForCommandSocket(socket: ServerWebSocket): Connection?

    /**
     * Get the connection associated with an update socket
     * @return The full Connection object or null if not found
     */
    fun getConnectionForUpdateSocket(socket: ServerWebSocket): Connection?

    /**
     * Remove a command socket from the cache
     */
    fun removeCommandSocket(connectionId: String): ServerWebSocket?

    /**
     * Remove an update socket from the cache
     */
    fun removeUpdateSocket(connectionId: String): ServerWebSocket?

    /**
     * Get all connection IDs with at least a command socket
     */
    fun getAllConnectedIds(): List<String>

    /**
     * Get number of connections with at least a command socket
     */
    fun getConnectionCount(): Int

    /**
     * Get number of connections with both command and update sockets
     */
    fun getFullyConnectedCount(): Int

    /**
     * Check if a connection has both command and update sockets
     */
    fun isFullyConnected(connectionId: String): Boolean
}