package com.minare.core.websocket

import com.minare.controller.ConnectionController
import com.minare.persistence.ConnectionStore
import io.vertx.core.Handler
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.coroutines.CoroutineContext

/**
 * Manages command WebSocket connections from clients.
 * Command WebSockets are used to receive client requests.
 */
@Singleton
class CommandSocketManager @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionController: ConnectionController,
    private val messageHandler: CommandMessageHandler,
    private val coroutineContext: CoroutineContext
) : Handler<ServerWebSocket> {

    private val log = LoggerFactory.getLogger(CommandSocketManager::class.java)

    override fun handle(websocket: ServerWebSocket) {
        log.info("New command WebSocket connection from {}", websocket.remoteAddress())

        websocket.textMessageHandler { message ->
            CoroutineScope(coroutineContext).launch {
                try {
                    val msg = JsonObject(message)
                    handleMessage(websocket, msg)
                } catch (e: Exception) {
                    handleError(websocket, e)
                }
            }
        }

        websocket.closeHandler {
            CoroutineScope(coroutineContext).launch {
                handleClose(websocket)
            }
        }
        websocket.accept()

        // Initiate command socket connection automatically
        CoroutineScope(coroutineContext).launch {
            initiateConnection(websocket)
        }
    }

    private suspend fun initiateConnection(websocket: ServerWebSocket) {
        try {
            val connection = connectionStore.create()
            connectionController.registerCommandSocket(connection._id, websocket)
            sendConnectionConfirmation(websocket, connection._id)

            // After connection is established, trigger sync
            connectionController.syncConnection(connection._id)
        } catch (e: Exception) {
            handleError(websocket, e)
        }
    }

    private fun sendConnectionConfirmation(websocket: ServerWebSocket, connectionId: String) {
        val confirmation = JsonObject()
            .put("type", "connection_confirm")
            .put("connectionId", connectionId)
            .put("timestamp", System.currentTimeMillis())

        websocket.writeTextMessage(confirmation.encode())
        log.info("Command socket established for {}", connectionId)
    }

    private suspend fun handleMessage(websocket: ServerWebSocket, message: JsonObject) {
        try {
            val connection = connectionController.getConnectionForCommandSocket(websocket)
            if (connection != null) {
                messageHandler.handle(connection._id, message)
            } else {
                throw IllegalStateException("No connection found for this websocket")
            }
        } catch (e: Exception) {
            handleError(websocket, e)
        }
    }

    private suspend fun handleClose(websocket: ServerWebSocket) {
        try {
            val connection = connectionController.getConnectionForCommandSocket(websocket)
            if (connection != null) {
                // Notify the update socket manager to close the update socket if it exists
                connectionController.handleCommandSocketClosed(connection._id)
            }
        } catch (e: Exception) {
            log.error("Error handling websocket close", e)
        }
    }

    private fun handleError(websocket: ServerWebSocket, error: Throwable) {
        log.error("Error in command WebSocket connection", error)
        try {
            val errorMessage = JsonObject()
                .put("type", "error")
                .put("message", error.message)

            websocket.writeTextMessage(errorMessage.encode())
        } finally {
            if (!websocket.isClosed()) {
                websocket.close()
            }
        }
    }
}