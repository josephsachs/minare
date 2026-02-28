package com.minare.core.transport.adapters

import com.minare.core.transport.interfaces.SocketStore
import io.vertx.core.http.ServerWebSocket
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

class WebsocketStore : SocketStore<ServerWebSocket> {
    private val log = LoggerFactory.getLogger(WebsocketStore::class.java)

    private val socketToConnectionId = ConcurrentHashMap<ServerWebSocket, String>()
    private val connectionToSocket = ConcurrentHashMap<String, ServerWebSocket>()

    override fun put(connectionId: String, socket: ServerWebSocket) {
        connectionToSocket.put(connectionId, socket)?.let { evicted ->
            socketToConnectionId.remove(evicted)
        }
        socketToConnectionId[socket] = connectionId
    }

    override fun getConnectionId(socket: ServerWebSocket): String? {
        return socketToConnectionId[socket]
    }

    override fun get(connectionId: String): ServerWebSocket? {
        return connectionToSocket[connectionId]
    }

    override fun remove(connectionId: String): ServerWebSocket? {
        val socket = connectionToSocket.remove(connectionId) ?: return null
        socketToConnectionId.remove(socket)
        return socket
    }

    override fun remove(socket: ServerWebSocket): Boolean {
        val connectionId = socketToConnectionId.remove(socket) ?: return false
        connectionToSocket.remove(connectionId)
        return true
    }

    override fun count(): Int = connectionToSocket.size

    override fun send(connectionId: String, message: String): Boolean {
        val socket = connectionToSocket[connectionId] ?: return false
        return try {
            socket.writeTextMessage(message)
            true
        } catch (e: Exception) {
            log.warn("Failed to send message to {}: {}", connectionId, e.message)
            false
        }
    }

    override fun close(connectionId: String): Boolean {
        val socket = connectionToSocket[connectionId] ?: return false
        return try {
            if (!socket.isClosed) {
                socket.close()
            }
            remove(connectionId)
            true
        } catch (e: Exception) {
            log.warn("Failed to close socket for {}: {}", connectionId, e.message)
            remove(connectionId)
            false
        }
    }

    override fun allConnectionIds(): Set<String> = connectionToSocket.keys.toSet()
}