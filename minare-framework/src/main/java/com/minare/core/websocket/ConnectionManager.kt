package com.minare.core.websocket

import io.vertx.core.Future
import io.vertx.core.http.ServerWebSocket
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Singleton

@Singleton
class ConnectionManager {

    private val log = LoggerFactory.getLogger(ConnectionManager::class.java)
    private val commandSockets = ConcurrentHashMap<String, ServerWebSocket>()
    private val updateSockets = ConcurrentHashMap<String, ServerWebSocket>()

    private val commandSocketToConnectionId = ConcurrentHashMap<ServerWebSocket, String>()
    private val updateSocketToConnectionId = ConcurrentHashMap<ServerWebSocket, String>()

    /**
     * Register a command WebSocket for a connection
     */
    fun registerCommandSocket(connectionId: String, websocket: ServerWebSocket): Future<Void> {
        return try {
            // Remove any existing socket for this connection
            val existingSocket = commandSockets.put(connectionId, websocket)
            if (existingSocket != null) {
                commandSocketToConnectionId.remove(existingSocket)
                try {
                    existingSocket.close()
                } catch (e: Exception) {
                    log.warn("Failed to close existing socket for {}", connectionId, e)
                }
            }

            commandSocketToConnectionId[websocket] = connectionId
            Future.succeededFuture()
        } catch (e: Exception) {
            log.error("Failed to register command WebSocket", e)
            Future.failedFuture(e)
        }
    }

    /**
     * Register an update WebSocket for a connection
     */
    fun registerUpdateSocket(connectionId: String, websocket: ServerWebSocket): Future<Void> {
        return try {
            // Remove any existing socket for this connection
            val existingSocket = updateSockets.put(connectionId, websocket)
            if (existingSocket != null) {
                updateSocketToConnectionId.remove(existingSocket)
                try {
                    existingSocket.close()
                } catch (e: Exception) {
                    log.warn("Failed to close existing update socket for {}", connectionId, e)
                }
            }

            updateSocketToConnectionId[websocket] = connectionId
            Future.succeededFuture()
        } catch (e: Exception) {
            log.error("Failed to register update WebSocket", e)
            Future.failedFuture(e)
        }
    }

    /**
     * Get the command WebSocket for a connection
     */
    fun getCommandSocket(connectionId: String): Future<ServerWebSocket?> {
        return Future.succeededFuture(commandSockets[connectionId])
    }

    /**
     * Get the update WebSocket for a connection
     */
    fun getUpdateSocket(connectionId: String): Future<ServerWebSocket?> {
        return Future.succeededFuture(updateSockets[connectionId])
    }

    /**
     * Get the connection ID for a command WebSocket
     */
    fun getConnectionIdForCommandSocket(websocket: ServerWebSocket): Future<String?> {
        return Future.succeededFuture(commandSocketToConnectionId[websocket])
    }

    /**
     * Get the connection ID for an update WebSocket
     */
    fun getConnectionIdForUpdateSocket(websocket: ServerWebSocket): Future<String?> {
        return Future.succeededFuture(updateSocketToConnectionId[websocket])
    }

    /**
     * Remove a command WebSocket
     */
    fun removeCommandSocket(connectionId: String): Future<Void> {
        try {
            val socket = commandSockets.remove(connectionId)
            if (socket != null) {
                commandSocketToConnectionId.remove(socket)
                try {
                    socket.close()
                } catch (e: Exception) {
                    log.warn("Error closing command socket for {}", connectionId, e)
                }
            }
            return Future.succeededFuture()
        } catch (e: Exception) {
            log.error("Failed to remove command WebSocket", e)
            return Future.failedFuture(e)
        }
    }

    /**
     * Remove an update WebSocket
     */
    fun removeUpdateSocket(connectionId: String): Future<Void> {
        try {
            val socket = updateSockets.remove(connectionId)
            if (socket != null) {
                updateSocketToConnectionId.remove(socket)
                try {
                    socket.close()
                } catch (e: Exception) {
                    log.warn("Error closing update socket for {}", connectionId, e)
                }
            }
            return Future.succeededFuture()
        } catch (e: Exception) {
            log.error("Failed to remove update WebSocket", e)
            return Future.failedFuture(e)
        }
    }

    /**
     * Handle the closure of a command socket by cleaning up both sockets
     */
    fun handleCommandSocketClosed(connectionId: String): Future<Void> {
        return removeCommandSocket(connectionId)
            .compose { removeUpdateSocket(connectionId) }
    }

    /**
     * Get all connected client IDs
     */
    fun getAllConnectedIds(): Future<List<String>> {
        return Future.succeededFuture(commandSockets.keys().toList())
    }

    /**
     * Get count of active connections
     */
    fun getConnectionCount(): Int {
        return commandSockets.size
    }

    /**
     * Get count of connections with both sockets established
     */
    fun getFullyConnectedCount(): Int {
        return commandSockets.keys.count { updateSockets.containsKey(it) }
    }
}