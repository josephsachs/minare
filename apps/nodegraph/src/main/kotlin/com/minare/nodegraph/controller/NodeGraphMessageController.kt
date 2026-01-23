package com.minare.nodegraph.controller

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.controller.MessageController
import com.minare.core.transport.models.Connection
import com.minare.core.transport.models.message.HeartbeatResponse
import com.minare.core.transport.models.message.OperationCommand
import com.minare.core.transport.models.message.SyncCommand
import com.minare.core.transport.models.message.SyncCommandType
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

@Singleton
class NodeGraphMessageController @Inject constructor() : MessageController() {
    private val log = LoggerFactory.getLogger(NodeGraphMessageController::class.java)

    override suspend fun handle(connection: Connection, message: JsonObject) {
        when {
            // We need to implement heartbeat responses or our connections will get cleaned up automatically
            message.getString("type") == "heartbeat_response" -> {
                val heartbeatCommand = HeartbeatResponse(
                    connection,
                    message.getLong("timestamp"),
                    message.getLong("clientTimestamp")
                )

                dispatch(heartbeatCommand)
            }

            message.getString("command") == "sync" -> {
                val syncCommand = SyncCommand(
                    connection,
                    SyncCommandType.CHANNEL,
                    null    // Sync all channels; our NodeGraph app only has one
                )

                dispatch(syncCommand)
            }

            // Assuming rather dangerously that any message NOS is an operation
            else -> {
                val operationCommand = OperationCommand(message)

                dispatch(operationCommand)
            }
        }
    }
}