package com.minare.cache

import com.minare.core.models.Connection
import io.vertx.core.http.ServerWebSocket
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Singleton

/**
 * In-memory implementation of ConnectionCache using ConcurrentHashMaps
 */
@Singleton
class InMemoryConnectionCache : ConnectionCache {
    // Connection storage
    private val connections = ConcurrentHashMap<String, Connection>()

    // Socket storage
    private val commandSockets = ConcurrentHashMap<String, ServerWebSocket>()
    private val updateSockets = ConcurrentHashMap<String, ServerWebSocket>()

    // Bidirectional mapping
    private val commandSocketToConnection = ConcurrentHashMap<ServerWebSocket, Connection>()
    private val updateSocketToConnection = ConcurrentHashMap<ServerWebSocket, Connection>()

    override fun storeConnection(connection: Connection) {
        connections[connection.id] = connection
    }

    override fun removeConnection(connectionId: String) {
        connections.remove(connectionId)
    }

    override fun getConnection(connectionId: String): Connection? {
        return connections[connectionId]
    }

    override fun hasConnection(connectionId: String): Boolean {
        return connections.containsKey(connectionId)
    }

    override fun storeCommandSocket(connectionId: String, socket: ServerWebSocket, connection: Connection) {
        // Remove old socket mapping if exists
        commandSockets[connectionId]?.let { oldSocket ->
            commandSocketToConnection.remove(oldSocket)
        }

        // Store new socket
        commandSockets[connectionId] = socket
        commandSocketToConnection[socket] = connection
    }

    override fun storeUpdateSocket(connectionId: String, socket: ServerWebSocket, connection: Connection) {
        // Remove old socket mapping if exists
        updateSockets[connectionId]?.let { oldSocket ->
            updateSocketToConnection.remove(oldSocket)
        }

        // Store new socket
        updateSockets[connectionId] = socket
        updateSocketToConnection[socket] = connection
    }

    override fun getCommandSocket(connectionId: String): ServerWebSocket? {
        return commandSockets[connectionId]
    }

    override fun getUpdateSocket(connectionId: String): ServerWebSocket? {
        return updateSockets[connectionId]
    }

    override fun getConnectionIdForCommandSocket(socket: ServerWebSocket): String? {
        return commandSocketToConnection[socket]?.id
    }

    override fun getConnectionIdForUpdateSocket(socket: ServerWebSocket): String? {
        return updateSocketToConnection[socket]?.id
    }

    override fun getConnectionForCommandSocket(socket: ServerWebSocket): Connection? {
        return commandSocketToConnection[socket]
    }

    override fun getConnectionForUpdateSocket(socket: ServerWebSocket): Connection? {
        return updateSocketToConnection[socket]
    }

    override fun removeCommandSocket(connectionId: String): ServerWebSocket? {
        val socket = commandSockets.remove(connectionId)
        socket?.let { commandSocketToConnection.remove(it) }
        return socket
    }

    override fun removeUpdateSocket(connectionId: String): ServerWebSocket? {
        val socket = updateSockets.remove(connectionId)
        socket?.let { updateSocketToConnection.remove(it) }
        return socket
    }

    override fun getAllConnectedIds(): List<String> {
        return commandSockets.keys().toList()
    }

    override fun getConnectionCount(): Int {
        return commandSockets.size
    }

    override fun getFullyConnectedCount(): Int {
        return commandSockets.keys.count { updateSockets.containsKey(it) }
    }

    override fun isFullyConnected(connectionId: String): Boolean {
        return commandSockets.containsKey(connectionId) && updateSockets.containsKey(connectionId)
    }
}