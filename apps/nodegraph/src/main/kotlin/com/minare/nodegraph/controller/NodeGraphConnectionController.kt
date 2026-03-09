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
     * If the connection carries `enable_metrics` in its meta, also subscribes
     * to the metrics broadcast channel.
     */
    override suspend fun onConnected(connection: Connection) {
        log.info("Client {} is now fully connected", connection.id)

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

        // ── Metrics channel subscription ──
        if (connection.meta?.get("enable_metrics") == "true") {
            val metricsChannelId = channelController.getMetricsChannel()
            if (metricsChannelId != null) {
                if (channelController.addClient(connection.id, metricsChannelId)) {
                    log.info("Client {} subscribed to metrics channel {}", connection.id, metricsChannelId)
                }
            } else {
                log.warn("Metrics channel not initialized; client {} requested metrics but cannot subscribe", connection.id)
            }
        }
    }
}