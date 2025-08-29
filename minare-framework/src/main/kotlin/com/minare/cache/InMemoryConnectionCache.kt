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

    private val upSockets = ConcurrentHashMap<String, ServerWebSocket>()
    private val downSockets = ConcurrentHashMap<String, ServerWebSocket>()

    private val upSocketToConnection = ConcurrentHashMap<ServerWebSocket, Connection>()
    private val downSocketToConnection = ConcurrentHashMap<ServerWebSocket, Connection>()

    override fun storeConnection(connection: Connection) {
        log.info("Storing connection in cache: {} with upSocketId={}, downSocketId={}",
            connection._id, connection.upSocketId, connection.downSocketId)
        connections[connection._id] = connection
    }

    override fun removeConnection(connectionId: String) {
        connections.remove(connectionId)
    }

    override suspend fun getConnection(connectionId: String): Connection? {
        val cachedConnection = connections[connectionId]

        if (cachedConnection != null) {
            log.info("Retrieved connection from cache: {} with upSocketId={}, downSocketId={}",
                cachedConnection._id, cachedConnection.upSocketId, cachedConnection.downSocketId)
            return cachedConnection
        }

        try {
            val connection = connectionStore.find(connectionId)
            log.info("Retrieved connection from database: {} with upSocketId={}, downSocketId={}",
                connection._id, connection.upSocketId, connection.downSocketId)

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

    override fun storeUpSocket(connectionId: String, socket: ServerWebSocket, connection: Connection) {
        upSockets[connectionId]?.let { oldSocket ->
            upSocketToConnection.remove(oldSocket)
        }
        upSockets[connectionId] = socket
        upSocketToConnection[socket] = connection
    }

    override fun storeDownSocket(connectionId: String, socket: ServerWebSocket, connection: Connection) {
        downSockets[connectionId]?.let { oldSocket ->
            downSocketToConnection.remove(oldSocket)
        }
        downSockets[connectionId] = socket
        downSocketToConnection[socket] = connection
    }

    override fun getUpSocket(connectionId: String): ServerWebSocket? {
        return upSockets[connectionId]
    }

    override fun getDownSocket(connectionId: String): ServerWebSocket? {
        return downSockets[connectionId]
    }

    override fun getConnectionIdForUpSocket(socket: ServerWebSocket): String? {
        return upSocketToConnection[socket]?._id
    }

    override fun getConnectionIdForDownSocket(socket: ServerWebSocket): String? {
        return downSocketToConnection[socket]?._id
    }

    override fun getConnectionForUpSocket(socket: ServerWebSocket): Connection? {
        return upSocketToConnection[socket]
    }

    override fun getConnectionForDownSocket(socket: ServerWebSocket): Connection? {
        return downSocketToConnection[socket]
    }

    override fun removeUpSocket(connectionId: String): ServerWebSocket? {
        val socket = upSockets.remove(connectionId)
        socket?.let { upSocketToConnection.remove(it) }
        return socket
    }

    override fun removeDownSocket(connectionId: String): ServerWebSocket? {
        val socket = downSockets.remove(connectionId)
        socket?.let { downSocketToConnection.remove(it) }
        return socket
    }

    override fun getAllConnectedIds(): List<String> {
        return upSockets.keys().toList()
    }

    override fun getConnectionCount(): Int {
        return upSockets.size
    }

    override fun getFullyConnectedCount(): Int {
        return upSockets.keys.count { downSockets.containsKey(it) }
    }

    override fun isFullyConnected(connectionId: String): Boolean {
        return upSockets.containsKey(connectionId) && downSockets.containsKey(connectionId)
    }
}