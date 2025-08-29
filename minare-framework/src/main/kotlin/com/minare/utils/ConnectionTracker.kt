package com.minare.utils

import com.google.inject.Inject
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * Tracks WebSocket connections and their associated trace IDs.
 * Provides centralized connection tracking and state management for verticles.
 */
class ConnectionTracker @Inject constructor(
    private val componentName: String,
    private val logger: VerticleLogger
) {
    private val log = LoggerFactory.getLogger("$componentName.ConnectionTracker")

    private val connectionTraces = ConcurrentHashMap<String, String>()
    private val socketToConnectionId = ConcurrentHashMap<ServerWebSocket, String>()
    private val connectionToSocket = ConcurrentHashMap<String, ServerWebSocket>()

    /**
     * Register a connection with its trace
     *
     * @param connectionId Connection ID
     * @param traceId Trace ID
     * @param socket WebSocket for this connection
     */
    fun registerConnection(connectionId: String, traceId: String, socket: ServerWebSocket) {
        connectionTraces[connectionId] = traceId
        socketToConnectionId[socket] = connectionId
        connectionToSocket[connectionId] = socket

        log.debug("Registered connection $connectionId with socket ${socket.textHandlerID()}")
    }

    /**
     * Get trace ID for a connection
     *
     * @param connectionId Connection ID
     * @return Trace ID or null if not found
     */
    fun getTraceId(connectionId: String): String? {
        return connectionTraces[connectionId]
    }

    /**
     * Get connection ID for a socket
     *
     * @param socket WebSocket
     * @return Connection ID or null if not found
     */
    fun getConnectionId(socket: ServerWebSocket): String? {
        return socketToConnectionId[socket]
    }

    /**
     * Get socket for a connection
     *
     * @param connectionId Connection ID
     * @return WebSocket or null if not found
     */
    fun getSocket(connectionId: String): ServerWebSocket? {
        return connectionToSocket[connectionId]
    }

    /**
     * Remove a connection and associated resources
     *
     * @param connectionId Connection ID
     * @return true if removed, false if not found
     */
    fun removeConnection(connectionId: String): Boolean {
        val socket = connectionToSocket.remove(connectionId) ?: return false
        socketToConnectionId.remove(socket)
        connectionTraces.remove(connectionId)

        log.debug("Removed connection $connectionId")
        return true
    }

    /**
     * Handle a socket being closed
     *
     * @param socket WebSocket that was closed
     * @return Connection ID that was associated with this socket, or null
     */
    fun handleSocketClosed(socket: ServerWebSocket): String? {
        val connectionId = socketToConnectionId.remove(socket) ?: return null
        connectionToSocket.remove(connectionId)

        logger.getEventLogger().trace("SOCKET_CLOSED", mapOf(
            "socketId" to socket.textHandlerID(),
            "connectionId" to connectionId
        ))

        log.debug("Handled close of socket for connection $connectionId")
        return connectionId
    }

    /**
     * Get all registered connection IDs
     *
     * @return Set of connection IDs
     */
    fun getAllConnectionIds(): Set<String> {
        return connectionTraces.keys
    }

    /**
     * Get count of registered connections
     *
     * @return Number of connections
     */
    fun getConnectionCount(): Int {
        return connectionTraces.size
    }

    /**
     * Create a JSON object with metrics about connections
     *
     * @return JsonObject with metrics
     */
    fun getMetrics(): JsonObject {
        return JsonObject()
            .put("connections", JsonObject()
                .put("total", connectionTraces.size)
                .put("active", connectionToSocket.size)
            )
    }
}