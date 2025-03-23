package com.minare.cache

import com.minare.core.models.Connection
import com.minare.persistence.ConnectionStore
import io.vertx.core.http.ServerWebSocket
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import javax.inject.Singleton

/**
 * In-memory implementation of ConnectionCache using ConcurrentHashMaps
 */
@Singleton
class InMemoryConnectionCache @Inject constructor(
    private val connectionStore: ConnectionStore
) : ConnectionCache {
    private val log = LoggerFactory.getLogger(InMemoryConnectionCache::class.java)
    private val connections = ConcurrentHashMap<String, Connection>()

    private val commandSockets = ConcurrentHashMap<String, ServerWebSocket>()
    private val updateSockets = ConcurrentHashMap<String, ServerWebSocket>()

    private val commandSocketToConnection = ConcurrentHashMap<ServerWebSocket, Connection>()
    private val updateSocketToConnection = ConcurrentHashMap<ServerWebSocket, Connection>()

    override fun storeConnection(connection: Connection) {
        log.info("[TRACE] Storing connection in cache: {} with commandSocketId={}, updateSocketId={}",
            connection._id, connection.commandSocketId, connection.updateSocketId)
        connections[connection._id] = connection
    }

    override fun removeConnection(connectionId: String) {
        connections.remove(connectionId)
    }

    override suspend fun getConnection(connectionId: String): Connection? {
        // First check in the local cache map
        val cachedConnection = connections[connectionId]

        if (cachedConnection != null) {
            log.info("[TRACE] Retrieved connection from cache: {} with commandSocketId={}, updateSocketId={}",
                cachedConnection._id, cachedConnection.commandSocketId, cachedConnection.updateSocketId)
            return cachedConnection
        }

        // Not in cache, look up in database
        try {
            val connection = connectionStore.find(connectionId)
            log.info("[TRACE] Retrieved connection from database: {} with commandSocketId={}, updateSocketId={}",
                connection._id, connection.commandSocketId, connection.updateSocketId)

            // Store in cache for future use
            connections[connectionId] = connection
            return connection
        } catch (e: Exception) {
            log.error("Failed to retrieve connection from database: {}", connectionId, e)
            return null
        }
    }

    override suspend fun hasConnection(connectionId: String): Boolean {
        return connections.containsKey(connectionId) || try {
            connectionStore.exists(connectionId)
        } catch (e: Exception) {
            false
        }
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
        return commandSocketToConnection[socket]?._id
    }

    override fun getConnectionIdForUpdateSocket(socket: ServerWebSocket): String? {
        return updateSocketToConnection[socket]?._id
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