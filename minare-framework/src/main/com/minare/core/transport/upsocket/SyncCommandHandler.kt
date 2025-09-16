package com.minare.worker.upsocket

import com.minare.cache.ConnectionCache
import com.minare.controller.ConnectionController
import com.minare.core.transport.models.message.SyncCommand
import com.minare.core.transport.models.message.SyncCommandType
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Handles sync commands outside of the Kafka flow.
 * TODO: Route sync thru downsocket instead of returning
 */
@Singleton
class SyncCommandHandler @Inject constructor(
    private val connectionController: ConnectionController,
    private val vertx: Vertx,
    private val connectionCache: ConnectionCache
) {
    private val log = LoggerFactory.getLogger(SyncCommandHandler::class.java)

    /**
     * Handle a sync command
     * Sync commands are used to request the current state of data
     * @deprecated route through MessageController
     */
    suspend fun handle(connectionId: String, message: JsonObject) {
        log.debug("Handling sync command for connection {}: {}", connectionId, message)

        // Check if this is a full channel sync request (no entity specified)
        val entityObject = message.getJsonObject("entity")

        if (entityObject == null) {
            log.info("Full channel sync requested for connection: {}", connectionId)

            val success = connectionController.syncConnection(connectionId)

            val response = JsonObject()
                .put("type", "sync_initiated")
                .put("success", success)
                .put("timestamp", System.currentTimeMillis())

            val upSocket = connectionCache.getUpSocket(connectionId)
            if (upSocket != null && !upSocket.isClosed()) {
                upSocket.writeTextMessage(response.encode())
            } else {
                log.warn("Cannot send sync initiated response: up socket not found or closed for {}", connectionId)
            }

            return
        }

        val id = entityObject.getString("_id") ?: entityObject.getString("id")

        if (id == null) {
            log.error("Sync command missing entity ID")
            sendErrorToClient(connectionId, "Entity ID is required for entity sync")
            return
        }

        log.debug("Entity-specific sync not yet implemented for entity: {}", id)
        sendErrorToClient(connectionId, "Entity-specific sync not yet implemented")
    }

    /**
     * Send an error response to the client
     * @deprecated use MessageController
     */
    private suspend fun sendErrorToClient(connectionId: String, errorMessage: String) {
        val response = JsonObject()
            .put("type", "sync_error")
            .put("error", errorMessage)

        val socket = connectionCache.getUpSocket(connectionId)
        if (socket != null && !socket.isClosed()) {
            socket.writeTextMessage(response.encode())
        } else {
            log.warn("Cannot send error response: up socket not found or closed for {}", connectionId)
        }
    }

    /**
     * Receives messsage from MessageController and invokes connectionController appropriately
     */
    suspend fun tryHandle(message: SyncCommand) {
        if (message.type == SyncCommandType.CHANNEL) {
            if (message.select.isNullOrEmpty()) {
                connectionController.syncConnection(message.connection._id)
            } else {
                throw NotImplementedError("Sync specific channel not implemented")
            }
        } else if (message.type == SyncCommandType.ENTITY) {
            throw NotImplementedError("Entity sync not implemented")
        } else {
            throw Exception("Unrecognized type")
        }
    }
}