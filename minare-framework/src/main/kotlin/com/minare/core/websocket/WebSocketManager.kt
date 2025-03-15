package com.minare.core.websocket

import com.minare.persistence.ConnectionStore
import io.vertx.core.Handler
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class WebSocketManager @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionManager: ConnectionManager,
    private val messageHandler: WebSocketMessageHandler
) : Handler<ServerWebSocket> {

    private val log = LoggerFactory.getLogger(WebSocketManager::class.java)

    override fun handle(websocket: ServerWebSocket) {
        log.info("New WebSocket connection from {}", websocket.remoteAddress())

        websocket.textMessageHandler { message ->
            try {
                val msg = JsonObject(message)
                handleMessage(websocket, msg)
            } catch (e: Exception) {
                handleError(websocket, e)
            }
        }

        websocket.closeHandler { handleClose(websocket) }
        websocket.accept()

        // Initiate handshake automatically for any connection
        // initiateHandshake(websocket)
    }

    /**private fun initiateHandshake(websocket: ServerWebSocket) {
        connectionStore.create()
            .flatMap { connection ->
                connectionManager.registerWebSocket(connection.id, websocket)
                    .map { connection }
            }
            .onSuccess { connection -> sendHandshakeConfirmation(websocket, connection.id) }
            .onFailure { err -> handleError(websocket, err) }
    }**/

    private fun sendHandshakeConfirmation(websocket: ServerWebSocket, connectionId: String) {
        val confirmation = JsonObject()
            .put("type", "handshake_confirm")
            .put("connectionId", connectionId)
            .put("timestamp", System.currentTimeMillis())

        websocket.writeTextMessage(confirmation.encode())
        log.info("Handshake completed for connection {}", connectionId)
    }

    private fun handleMessage(websocket: ServerWebSocket, message: JsonObject) {
        /**connectionManager.getConnectionIdForWebSocket(websocket)
            .compose { connectionId ->
                if (connectionId != null) {
                    messageHandler.handle(connectionId, message)
                } else {
                    Future.failedFuture<Void>("No connection ID found for this websocket")
                }
            }
            .onFailure { err -> handleError(websocket, err) }**/
    }

    private fun handleClose(websocket: ServerWebSocket) {
        /**connectionManager.getConnectionIdForWebSocket(websocket)
            .compose { connectionId ->
                if (connectionId != null) {
                    connectionManager.removeWebSocket(connectionId)
                        .compose { connectionStore.delete(connectionId) }
                } else {
                    Future.succeededFuture()
                }
            }
            .onFailure { err -> log.error("Error handling websocket close", err) }**/
    }

    private fun handleError(websocket: ServerWebSocket, error: Throwable) {
        log.error("Error in WebSocket connection", error)
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