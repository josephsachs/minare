package com.minare.worker.upsocket.handlers

import com.google.inject.Inject
import com.minare.persistence.ConnectionStore
import com.minare.utils.ConnectionTracker
import com.minare.utils.HeartbeatManager
import com.minare.utils.VerticleLogger
import com.minare.utils.WebSocketUtils
import com.minare.controller.OperationController
import com.minare.worker.upsocket.SyncCommandHandler
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import kotlin.com.minare.exception.BackpressureException

/**
 * Handles incoming messages from WebSocket connections.
 * Routes messages to appropriate handlers based on type:
 * - Heartbeats: handled directly
 * - Sync commands: routed to SyncCommandHandler (temporary)
 * - All others: routed to OperationController for Kafka
 */
class MessageHandler @Inject constructor(
    private val vlog: VerticleLogger,
    private val connectionStore: ConnectionStore,
    private val connectionTracker: ConnectionTracker,
    private val heartbeatManager: HeartbeatManager,
    private val operationController: OperationController,
    private val syncCommandHandler: SyncCommandHandler
) {
    /**
     * Handle an incoming message from a client
     */
    suspend fun handle(websocket: ServerWebSocket, message: JsonObject) {
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
                "command" to message.getString("command", "unknown"),
                "connectionId" to connectionId
            ), traceId
        )

        try {
            connectionStore.updateLastActivity(connectionId)

            // Route based on message type
            when {
                // Heartbeat responses are handled directly
                message.getString("type") == "heartbeat_response" -> {
                    heartbeatManager.handleHeartbeatResponse(connectionId, message)
                }

                // Sync commands bypass Kafka (temporary implementation)
                message.getString("command") == "sync" -> {
                    vlog.logInfo("Routing sync command to SyncCommandHandler for connection ${connectionId}")
                    syncCommandHandler.handle(connectionId, message)
                }

                // All other messages go through OperationController to Kafka
                else -> {
                    // Add connectionId to the message for downstream processing
                    message.put("connectionId", connectionId)

                    // All other messages produce to Kafka
                    try {
                        operationController.process(message)

                    } catch (e: BackpressureException) {
                        // Send 503 to client
                        val errorResponse = JsonObject()
                            .put("type", "error")
                            .put("code", 503)
                            .put("message", "Service temporarily unavailable - system at capacity")
                            .put("retry_after", 5) // seconds

                        websocket.writeTextMessage(errorResponse.encode())
                    }
                }
            }

            vlog.getEventLogger().trace(
                "MESSAGE_PROCESSED", mapOf(
                    "messageType" to message.getString("type", "unknown"),
                    "command" to message.getString("command", "unknown"),
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