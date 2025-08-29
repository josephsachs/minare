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
     * Associate an up socket with a connection
     * @param connectionId The connection ID
     * @param socket The WebSocket instance
     * @param connection The full Connection object
     */
    fun storeUpSocket(connectionId: String, socket: ServerWebSocket, connection: Connection)

    /**
     * Associate an down socket with a connection
     * @param connectionId The connection ID
     * @param socket The WebSocket instance
     * @param connection The full Connection object
     */
    fun storeDownSocket(connectionId: String, socket: ServerWebSocket, connection: Connection)

    /**
     * Get the up socket for a connection
     */
    fun getUpSocket(connectionId: String): ServerWebSocket?

    /**
     * Get the down socket for a connection
     */
    fun getDownSocket(connectionId: String): ServerWebSocket?

    /**
     * Get the connection ID associated with an up socket
     * @return The connection ID or null if not found
     */
    fun getConnectionIdForUpSocket(socket: ServerWebSocket): String?

    /**
     * Get the connection ID associated with an down socket
     * @return The connection ID or null if not found
     */
    fun getConnectionIdForDownSocket(socket: ServerWebSocket): String?

    /**
     * Get the connection associated with an up socket
     * @return The full Connection object or null if not found
     */
    fun getConnectionForUpSocket(socket: ServerWebSocket): Connection?

    /**
     * Get the connection associated with an down socket
     * @return The full Connection object or null if not found
     */
    fun getConnectionForDownSocket(socket: ServerWebSocket): Connection?

    /**
     * Remove an up socket from the cache
     */
    fun removeUpSocket(connectionId: String): ServerWebSocket?

    /**
     * Remove an down socket from the cache
     */
    fun removeDownSocket(connectionId: String): ServerWebSocket?

    /**
     * Get all connection IDs with at least an up socket
     */
    fun getAllConnectedIds(): List<String>

    /**
     * Get number of connections with at least an up socket
     */
    fun getConnectionCount(): Int

    /**
     * Get number of connections with both up and down sockets
     */
    fun getFullyConnectedCount(): Int

    /**
     * Check if a connection has both up and down sockets
     */
    fun isFullyConnected(connectionId: String): Boolean
}