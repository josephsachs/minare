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
 * Manages update WebSocket connections to clients.
 * Update sockets are used for pushing updates from server to clients.
 */
@Singleton
class UpdateSocketManager @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionController: ConnectionController,
    private val coroutineContext: CoroutineContext
) : Handler<ServerWebSocket> {

    private val log = LoggerFactory.getLogger(UpdateSocketManager::class.java)

    override fun handle(websocket: ServerWebSocket) {
        log.info("New update socket connection from {}", websocket.remoteAddress())

        websocket.textMessageHandler { message ->
            CoroutineScope(coroutineContext).launch {
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
        }

        websocket.closeHandler {
            CoroutineScope(coroutineContext).launch {
                try {
                    val connection = connectionController.getConnectionForUpdateSocket(websocket)
                    if (connection != null) {
                        connectionController.removeUpdateSocket(connection.id)
                    }
                } catch (e: Exception) {
                    log.error("Error handling update socket close", e)
                }
            }
        }

        websocket.accept()
    }

    private suspend fun associateUpdateSocket(connectionId: String, websocket: ServerWebSocket) {
        try {
            // Verify connection exists by trying to get it
            connectionController.getConnection(connectionId)

            // Associate the update socket
            connectionController.registerUpdateSocket(connectionId, websocket)

            // Send confirmation
            sendUpdateSocketConfirmation(websocket, connectionId)
        } catch (e: Exception) {
            handleError(websocket, e)
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

    /**
     * Send an update to a specific client via their update socket
     */
    suspend fun sendUpdate(connectionId: String, update: JsonObject): Boolean {
        val websocket = connectionController.getUpdateSocket(connectionId)

        return if (websocket != null) {
            try {
                websocket.writeTextMessage(update.encode())
                true
            } catch (e: Exception) {
                log.error("Failed to send update to {}", connectionId, e)
                false
            }
        } else {
            log.warn("No update socket found for connection {}", connectionId)
            false
        }
    }

    /**
     * Broadcast an update to specified clients
     */
    suspend fun broadcastUpdate(clientIds: List<String>, update: JsonObject): Int {
        var successCount = 0

        for (clientId in clientIds) {
            try {
                if (sendUpdate(clientId, update)) {
                    successCount++
                }
            } catch (e: Exception) {
                log.debug("Failed to send update to client {}: {}", clientId, e.message)
                // Continue with next client
            }
        }

        return successCount
    }

    /**
     * Broadcast an update to all connected clients
     */
    suspend fun broadcastUpdateAll(update: JsonObject): Int {
        val connectionIds = connectionController.getAllConnectedIds()
        return broadcastUpdate(connectionIds, update)
    }

    private fun handleError(websocket: ServerWebSocket, error: Throwable) {
        log.error("Error in update socket connection", error)
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