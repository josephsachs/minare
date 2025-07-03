package com.minare.worker.upsocket.handlers

import com.google.inject.Inject
import com.minare.persistence.ConnectionStore
import com.minare.utils.ConnectionTracker
import com.minare.utils.HeartbeatManager
import com.minare.utils.VerticleLogger
import com.minare.utils.WebSocketUtils
import com.minare.controller.OperationController
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject

/**
 * Handles incoming messages from WebSocket connections.
 * Refactored to delegate all command processing to OperationController
 * as part of the Kafka-based architecture.
 */
class MessageHandler @Inject constructor(
    private val vlog: VerticleLogger,
    private val connectionStore: ConnectionStore,
    private val connectionTracker: ConnectionTracker,
    private val heartbeatManager: HeartbeatManager,
    private val operationController: OperationController
) {
    /**
     * Handle an incoming message from a client
     */
    public suspend fun handle(websocket: ServerWebSocket, message: JsonObject) {
        val connectionId = connectionTracker.getConnectionId(websocket)
        if (connectionId == null) {
            WebSocketUtils.sendErrorResponse(
                websocket,
                IllegalStateException("No connection found for this websocket"), null, vlog
            )
            return
        }

        val traceId = connectionTracker.getTraceId(connectionId)
        val msgTraceId = vlog.getEventLogger().trace(
            "MESSAGE_RECEIVED", mapOf(
                "messageType" to message.getString("type", "unknown"),
                "connectionId" to connectionId
            ), traceId
        )

        try {
            connectionStore.updateLastActivity(connectionId)

            if (message.getString("type") == "heartbeat_response") {
                // Heartbeat responses are handled directly
                heartbeatManager.handleHeartbeatResponse(connectionId, message)
            } else {
                // All other messages are delegated to OperationController
                // Add connectionId to the message for downstream processing
                message.put("connectionId", connectionId)
                operationController.queue(message)
            }

            vlog.getEventLogger().trace(
                "MESSAGE_PROCESSED", mapOf(
                    "messageType" to message.getString("type", "unknown"),
                    "connectionId" to connectionId
                ), msgTraceId
            )
        } catch (e: Exception) {
            vlog.logVerticleError("MESSAGE_HANDLING", e)
            // Note: We don't send error responses for queue failures
            // The message is either processed or logged - no client feedback
            // This maintains the fire-and-forget pattern for Kafka
        }
    }
}