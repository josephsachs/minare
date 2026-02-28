package com.minare.controller

import com.google.inject.Inject
import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.coordinator.FrameCoordinatorState.Companion.PauseState
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.models.Connection
import com.minare.core.transport.models.message.*
import com.minare.core.transport.upsocket.handlers.SyncCommandHandler
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.exceptions.BackpressureException
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.UUID

abstract class MessageController @Inject constructor() {
    private val log = LoggerFactory.getLogger(MessageController::class.java)

    @Inject protected lateinit var connectionStore: ConnectionStore
    @Inject protected lateinit var connectionController: ConnectionController
    @Inject protected lateinit var syncCommandHandler: SyncCommandHandler
    @Inject protected lateinit var operationController: OperationController
    @Inject protected lateinit var coordinatorState: FrameCoordinatorState
    @Inject protected lateinit var vlog: VerticleLogger

    internal suspend fun handleUpsocket(connectionId: String?, message: JsonObject) {
        if (connectionId == null) {
            log.warn("Received message with no associated connection")
            return
        }

        connectionStore.updateLastActivity(connectionId)
        val connection = connectionController.getConnection(connectionId)

        try {
            handle(connection, message)
        } catch (e: BackpressureException) {
            connectionController.sendToUpSocket(connectionId, JsonObject()
                .put("type", "error")
                .put("code", "BACKPRESSURE")
                .put("message", e.message)
                .put("timestamp", System.currentTimeMillis())
            )
        }
    }

    suspend fun dispatch(message: CommandMessageObject) {
        when (message) {
            is HeartbeatResponse -> {
                // Heartbeat responses are informational; no action needed
            }

            is SyncCommand -> {
                val success = syncCommandHandler.tryHandle(message)

                connectionController.sendToUpSocket(message.connection.id, JsonObject()
                    .put("type", "sync_initiated")
                    .put("success", success)
                    .put("timestamp", System.currentTimeMillis())
                )
            }

            is OperationCommand -> {
                if (coordinatorState.pauseState == PauseState.HARD) {
                    throw BackpressureException("Service temporarily unavailable")
                }
                operationController.process(message.payload)
            }
        }
    }

    open suspend fun getTraceId(): String {
        return UUID.randomUUID().toString()
    }

    protected abstract suspend fun handle(connection: Connection, message: JsonObject)
}