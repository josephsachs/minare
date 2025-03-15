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
 * Manages command WebSocket connections from clients.
 * Command WebSockets are used to receive client requests.
 */
@Singleton
class CommandSocketManager @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionManager: ConnectionManager,
    private val messageHandler: CommandMessageHandler
) : Handler<ServerWebSocket> {

    private val log = LoggerFactory.getLogger(CommandSocketManager::class.java)

    override fun handle(websocket: ServerWebSocket) {
        log.info("New command WebSocket connection from {}", websocket.remoteAddress())

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

        // Initiate command socket connection automatically
        initiateConnection(websocket)
    }

    private fun initiateConnection(websocket: ServerWebSocket) {
        connectionStore.create()
            .flatMap { connection ->
                connectionManager.registerCommandSocket(connection.id, websocket)
                    .map { connection }
            }
            .onSuccess { connection -> sendConnectionConfirmation(websocket, connection.id) }
            .onFailure { err -> handleError(websocket, err) }
    }

    private fun sendConnectionConfirmation(websocket: ServerWebSocket, connectionId: String) {
        val confirmation = JsonObject()
            .put("type", "connection_confirm")
            .put("connectionId", connectionId)
            .put("timestamp", System.currentTimeMillis())

        websocket.writeTextMessage(confirmation.encode())
        log.info("Command socket established for {}", connectionId)
    }

    private fun handleMessage(websocket: ServerWebSocket, message: JsonObject) {
        connectionManager.getConnectionIdForCommandSocket(websocket)
            .compose { connectionId ->
                if (connectionId != null) {
                    messageHandler.handle(connectionId, message)
                } else {
                    Future.failedFuture<Void>("No connection ID found for this websocket")
                }
            }
            .onFailure { err -> handleError(websocket, err) }
    }

    private fun handleClose(websocket: ServerWebSocket) {
        connectionManager.getConnectionIdForCommandSocket(websocket)
            .compose { connectionId ->
                if (connectionId != null) {
                    // Notify the update socket manager to close the update socket if it exists
                    connectionManager.handleCommandSocketClosed(connectionId)
                        .compose { connectionStore.delete(connectionId) }
                } else {
                    Future.succeededFuture()
                }
            }
            .onFailure { err -> log.error("Error handling websocket close", err) }
    }

    private fun handleError(websocket: ServerWebSocket, error: Throwable) {
        log.error("Error in command WebSocket connection", error)
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