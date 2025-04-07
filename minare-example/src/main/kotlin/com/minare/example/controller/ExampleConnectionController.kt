package com.minare.example.controller

import com.minare.cache.ConnectionCache
import com.minare.controller.ConnectionController
import com.minare.core.entity.ReflectionCache
import com.minare.core.models.Connection
import com.minare.persistence.*
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Example-specific extension of ConnectionController that handles
 * channel subscriptions and graph synchronization on client connection.
 */
@Singleton
class ExampleConnectionController @Inject constructor(
    connectionStore: ConnectionStore,
    connectionCache: ConnectionCache,
    channelStore: ChannelStore,
    contextStore: ContextStore,
    entityStore: EntityStore,
    reflectionCache: ReflectionCache,
    private val channelController: ExampleChannelController
) : ConnectionController(
    connectionStore,
    connectionCache,
    channelStore,
    contextStore,
    entityStore,
    reflectionCache
) {
    private val log = LoggerFactory.getLogger(ExampleConnectionController::class.java)

    /**
     * Called when a client becomes fully connected.
     * Subscribes the client to the default channel and initiates sync.
     */
    override suspend fun onClientFullyConnected(connection: Connection) {
        log.info("Example client {} is now fully connected", connection._id)


        val defaultChannelId = channelController.getDefaultChannel()

        if (defaultChannelId == null) {
            log.warn("No default channel found for client {}, skipping auto-subscription", connection._id)
            return
        }


        if (channelController.subscribeClientToChannel(defaultChannelId, connection._id)) {

            syncChannelToConnection(defaultChannelId, connection._id)


            sendInitialSyncComplete(connection._id)
        }
    }

    /**
     * Send a message to the client indicating that initial sync is complete
     */
    private fun sendInitialSyncComplete(connectionId: String) {
        val commandSocket = getCommandSocket(connectionId)
        if (commandSocket != null && !commandSocket.isClosed()) {
            val message = JsonObject()
                .put("type", "initial_sync_complete")
                .put("timestamp", System.currentTimeMillis())

            commandSocket.writeTextMessage(message.encode())
            log.debug("Sent initial sync complete notification to client {}", connectionId)
        }
    }
}