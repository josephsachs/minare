package com.minare.worker.upsocket.handlers

import com.google.inject.Inject
import com.minare.utils.HeartbeatManager
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.coordinator.FrameCoordinatorState.Companion.PauseState
import com.minare.utils.WebSocketUtils
import com.minare.controller.OperationController
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.downsocket.services.ConnectionTracker
import com.minare.exceptions.BackpressureException
import com.minare.worker.upsocket.SyncCommandHandler
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject

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
    private val coordinatorState: FrameCoordinatorState,
    private val heartbeatManager: HeartbeatManager,
    private val operationController: OperationController,
    private val syncCommandHandler: SyncCommandHandler
) {
    /**
     * TODO: We need to implement MessageController, overridable handleMessage and MessageCommand /
     *     in the same way that we do with preQueue and Operation. That way the developer can /
     *     structure their client command arbitrarily.
     */

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
                    if (coordinatorState.pauseState == PauseState.HARD) {
                        throw BackpressureException("Service temporarily unavailable - system is paused")
                    }

                    // Add connectionId to the message for downstream processing
                    message.put("connectionId", connectionId)
                    operationController.process(message)
                }
            }

            vlog.getEventLogger().trace(
                "MESSAGE_PROCESSED", mapOf(
                    "messageType" to message.getString("type", "unknown"),
                    "command" to message.getString("command", "unknown"),
                    "connectionId" to connectionId
                ), msgTraceId
            )
        } catch (e: BackpressureException) {
            vlog.logInfo("Backpressure active, rejecting message from connection ${connectionId}")
            WebSocketUtils.sendErrorResponse(
                websocket,
                e, null, vlog
            )
        } catch (e: Exception) {
            vlog.logVerticleError("MESSAGE_HANDLING", e)
            // Note: We don't send error responses for queue failures
            // The message is either processed or logged - no client feedback
            // This maintains the fire-and-forget pattern for Kafka
        }
    }
}