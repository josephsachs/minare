package com.minare.core.websocket

import com.minare.persistence.ConnectionStore
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Manages update WebSocket connections to clients.
 * Update sockets are used for pushing updates from server to clients.
 */
@Singleton
class UpdateSocketManager @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionManager: ConnectionManager
) : Handler<ServerWebSocket> {

    private val log = LoggerFactory.getLogger(UpdateSocketManager::class.java)

    override fun handle(websocket: ServerWebSocket) {
        log.info("New update socket connection from {}", websocket.remoteAddress())

        websocket.textMessageHandler { message ->
            try {
                val msg = JsonObject(message)
                val connectionId = msg.getString("connectionId")

                if (connectionId != null) {
                    associateUpdateSocket(connectionId, websocket)
                } else {
                    handleError(websocket, IllegalArgumentException("No connectionId provided"))
                }
            } catch (e: Exception) {
                handleError(websocket, e)
            }
        }

        websocket.closeHandler {
            connectionManager.getConnectionIdForUpdateSocket(websocket)
                .compose { connectionId ->
                    if (connectionId != null) {
                        connectionManager.removeUpdateSocket(connectionId)
                            .compose {
                                connectionStore.updateUpdateSocketId(connectionId, null)
                            }
                    } else {
                        Future.succeededFuture()
                    }
                }
                .onFailure { err ->
                    log.error("Error handling update socket close", err)
                }
        }

        websocket.accept()
    }

    private fun associateUpdateSocket(connectionId: String, websocket: ServerWebSocket) {
        connectionStore.find(connectionId)
            .compose { connection ->
                connectionManager.registerUpdateSocket(connectionId, websocket)
                    .compose {
                        connectionStore.updateUpdateSocketId(connectionId, generateUpdateSocketId())
                    }
            }
            .onSuccess { updatedConnection ->
                sendUpdateSocketConfirmation(websocket, connectionId)
            }
            .onFailure { err ->
                handleError(websocket, err)
            }
    }

    private fun sendUpdateSocketConfirmation(websocket: ServerWebSocket, connectionId: String) {
        val confirmation = JsonObject()
            .put("type", "update_socket_confirm")
            .put("connectionId", connectionId)
            .put("timestamp", System.currentTimeMillis())

        websocket.writeTextMessage(confirmation.encode())
        log.info("Update socket established for connection {}", connectionId)
    }

    private fun generateUpdateSocketId(): String {
        return "us-" + java.util.UUID.randomUUID().toString()
    }

    /**
     * Send an update to a specific client via their update socket
     */
    fun sendUpdate(connectionId: String, update: JsonObject): Future<Void> {
        return connectionManager.getUpdateSocket(connectionId)
            .compose { websocket ->
                if (websocket != null) {
                    try {
                        websocket.writeTextMessage(update.encode())
                        Future.succeededFuture()
                    } catch (e: Exception) {
                        log.error("Failed to send update to {}", connectionId, e)
                        Future.failedFuture(e)
                    }
                } else {
                    log.warn("No update socket found for connection {}", connectionId)
                    Future.failedFuture("No update socket found")
                }
            }
    }

    /**
     * Broadcast an update to all connected clients
     */
    fun broadcastUpdate(update: JsonObject): Future<Void> {
        return connectionManager.getAllConnectedIds()
            .compose { connectionIds ->
                val futures = connectionIds.map { connectionId ->
                    sendUpdate(connectionId, update)
                        .otherwise { null } // Continue broadcasting even if some fail
                }

                Future.all(futures).map { null }
            }
    }

    private fun handleError(websocket: ServerWebSocket, error: Throwable) {
        log.error("Error in update socket connection", error)
        try {
            val errorMessage = JsonObject()
                .put("type", "error")
                .put("message", error.message)

            websocket.writeTextMessage(errorMessage.encode())
        } finally {
            websocket.close()
        }
    }
}