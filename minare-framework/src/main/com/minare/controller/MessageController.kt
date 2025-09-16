package com.minare.controller

import com.google.inject.Singleton
import com.google.inject.name.Named
import com.minare.cache.ConnectionCache
import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.coordinator.FrameCoordinatorState.Companion.PauseState
import com.minare.core.operation.interfaces.MessageQueue
import com.minare.core.operation.models.Operation
import com.minare.core.operation.models.OperationSet
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.downsocket.services.ConnectionTracker
import com.minare.core.transport.models.Connection
import com.minare.core.transport.models.message.*
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.exceptions.BackpressureException
import com.minare.utils.HeartbeatManager
import com.minare.utils.WebSocketUtils
import com.minare.worker.upsocket.SyncCommandHandler
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * Controller for the handling socket messages.
 *
 * This follows the framework pattern:
 * - Framework provides this open base class
 * - Applications must bind it in their module (to this class or their extension)
 * - Applications can extend this class to customize behavior
 */
abstract class MessageController @Inject constructor() {
    private val log = LoggerFactory.getLogger(MessageController::class.java)

    @Inject protected lateinit var connectionStore: ConnectionStore
    @Inject protected lateinit var connectionController: ConnectionController
    @Inject protected lateinit var connectionCache: ConnectionCache
    @Inject protected lateinit var syncCommandHandler: SyncCommandHandler
    @Inject protected lateinit var operationController: OperationController
    @Inject protected lateinit var coordinatorState: FrameCoordinatorState
    @Inject protected lateinit var vlog: VerticleLogger
    @Inject protected lateinit var heartbeatManager: HeartbeatManager

    /**
     * Process an incoming message from the WebSocket.
     * This is the entry point for messages to MessageHandler.
     *
     * @param message The raw message from the client
     */
    suspend fun dispatch(message: CommandMessageObject) {
        when (message) {
            is HeartbeatResponse -> {
                val payload = JsonObject()
                    .put("timestamp", message.timestamp)
                    .put("clientTimestamp", message.clientTimestamp)

                heartbeatManager.handleHeartbeatResponse(message.connection._id, payload)
            }

            is SyncCommand -> {
                val success = syncCommandHandler.tryHandle(message)

                val response = JsonObject()
                    .put("type", "sync_initiated")
                    .put("success", success)
                    .put("timestamp", System.currentTimeMillis())

                sendToUpSocket(message.connection, response)
            }

            is OperationCommand -> {
                if (coordinatorState.pauseState == PauseState.HARD) {
                    throw BackpressureException("Service temporarily unavailable")
                }

                operationController.process(message.payload)
            }
        }
    }

    suspend fun sendToUpSocket(connection: Connection, message: JsonObject) {
        val upSocket = connectionCache.getUpSocket(connection._id)
        if (upSocket != null && !upSocket.isClosed()) {
            upSocket.writeTextMessage(message.encode())
        } else {
            log.warn("Cannot send sync initiated response: up socket not found or closed for {}", connection._id)
        }
    }

    /**
     * Application developer override hook.
     * Convert incoming client messages to Operations.
     *
     * @param message The raw message from the client
     * @return Operation, OperationSet, or null to skip processing
     */
    internal suspend fun handleUpsocket(connectionTracker: ConnectionTracker, webSocket: ServerWebSocket, message: JsonObject) {
        val connectionId = connectionTracker.getConnectionId(webSocket)

        if (connectionId == null) {
            WebSocketUtils.sendErrorResponse(
                webSocket,
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

        connectionStore.updateLastActivity(connectionId)
        val connection = connectionController.getConnection(connectionId)

        handle(connection, message)
    }

    protected abstract suspend fun handle(connection: Connection, message: JsonObject)
}