package com.minare.worker.command.handlers

import com.google.inject.Inject
import com.minare.persistence.ConnectionStore
import com.minare.utils.ConnectionTracker
import com.minare.utils.HeartbeatManager
import com.minare.utils.VerticleLogger
import com.minare.utils.WebSocketUtils
import com.minare.worker.command.CommandMessageHandler
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject

class MessageHandler @Inject constructor(
    private val vlog: VerticleLogger,
    private val connectionStore: ConnectionStore,
    private val connectionTracker: ConnectionTracker,
    private val heartbeatManager: HeartbeatManager,
    private val commandMessageHandler: CommandMessageHandler
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
                heartbeatManager.handleHeartbeatResponse(connectionId, message)
            } else {
                commandMessageHandler.handle(connectionId, message)
            }

            vlog.getEventLogger().trace(
                "MESSAGE_PROCESSED", mapOf(
                    "messageType" to message.getString("type", "unknown"),
                    "connectionId" to connectionId
                ), msgTraceId
            )
        } catch (e: Exception) {
            vlog.logVerticleError("MESSAGE_HANDLING", e)
            WebSocketUtils.sendErrorResponse(websocket, e, connectionId, vlog)
        }
    }
}