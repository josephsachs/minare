package com.minare.integration.controller

import com.minare.controller.ConnectionController
import com.minare.core.transport.models.Connection
import com.minare.core.transport.upsocket.handlers.SyncCommandHandler
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class TestConnectionController @Inject constructor(
    private val channelController: TestChannelController,
    private val syncCommandHandler: SyncCommandHandler
) : ConnectionController() {
    private val log = LoggerFactory.getLogger(TestConnectionController::class.java)

    override suspend fun onClientFullyConnected(connection: Connection) {
        log.info("Test client {} is now fully connected", connection.id)

        val defaultChannelId = channelController.getDefaultChannel()

        if (defaultChannelId == null) {
            log.warn("No default channel found for client {}, skipping auto-subscription", connection.id)
            return
        }

        if (channelController.addClient(connection.id, defaultChannelId)) {
            syncCommandHandler.syncChannelToConnection(defaultChannelId, connection.id)
            sendToUpSocket(connection.id, JsonObject()
                .put("type", "initial_sync_complete")
                .put("timestamp", System.currentTimeMillis())
            )
        }
    }
}