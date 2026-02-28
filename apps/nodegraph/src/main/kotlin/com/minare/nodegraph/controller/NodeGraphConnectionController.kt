package com.minare.nodegraph.controller

import com.minare.controller.ConnectionController
import com.minare.core.transport.models.Connection
import com.minare.core.transport.upsocket.handlers.SyncCommandHandler
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Example-specific extension of ConnectionController that handles
 * channel subscriptions and graph synchronization on client connection.
 */
@Singleton
class NodeGraphConnectionController @Inject constructor(
    private val channelController: NodeGraphChannelController,
    private val syncCommandHandler: SyncCommandHandler
) : ConnectionController() {
    private val log = LoggerFactory.getLogger(NodeGraphConnectionController::class.java)

    /**
     * Called when a client becomes fully connected.
     * Subscribes the client to the default channel and initiates sync.
     */
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