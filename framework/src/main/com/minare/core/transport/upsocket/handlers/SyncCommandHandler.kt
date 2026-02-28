package com.minare.core.transport.upsocket.handlers

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.storage.interfaces.ContextStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.transport.models.message.SyncCommand
import com.minare.core.transport.models.message.SyncCommandType
import com.minare.core.transport.upsocket.UpSocketVerticle
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

@Singleton
class SyncCommandHandler @Inject constructor(
    private val vertx: Vertx,
    private val connectionStore: ConnectionStore,
    private val channelStore: ChannelStore,
    private val contextStore: ContextStore,
    private val stateStore: StateStore
) {
    private val log = LoggerFactory.getLogger(SyncCommandHandler::class.java)

    suspend fun tryHandle(message: SyncCommand): Boolean {
        return when (message.type) {
            SyncCommandType.CHANNEL -> {
                if (message.select.isNullOrEmpty()) {
                    syncConnection(message.connection.id)
                } else {
                    throw NotImplementedError("Sync specific channel not implemented")
                }
            }
            SyncCommandType.ENTITY -> {
                throw NotImplementedError("Entity sync not implemented")
            }
            else -> throw Exception("Unrecognized sync type")
        }
    }

    suspend fun syncChannelToConnection(channelId: String, connectionId: String) {
        try {
            val entityIds = contextStore.getEntityIdsByChannel(channelId)

            if (entityIds.isEmpty()) return

            val entities = stateStore.findJsonByIds(entityIds).values

            val syncMessage = JsonObject()
                .put("type", "sync")
                .put("data", JsonObject()
                    .put("entities", JsonArray(entities.toList()))
                    .put("channelId", channelId)
                    .put("timestamp", System.currentTimeMillis())
                )

            sendToUpSocket(connectionId, syncMessage)
        } catch (e: Exception) {
            log.error("Error syncing channel {} to connection {}", channelId, connectionId, e)
        }
    }

    private suspend fun syncConnection(connectionId: String): Boolean {
        try {
            val channels = channelStore.getChannelsForClient(connectionId)
            if (channels.isEmpty()) return true

            for (channelId in channels) {
                syncChannelToConnection(channelId, connectionId)
            }
            return true
        } catch (e: Exception) {
            log.error("Error during connection sync: {}", connectionId, e)
            return false
        }
    }

    private suspend fun sendToUpSocket(connectionId: String, message: JsonObject) {
        try {
            val connection = connectionStore.find(connectionId)
            val deploymentId = connection.upSocketDeploymentId
            if (deploymentId == null) {
                log.warn("No upSocketDeploymentId for connection {}, cannot send message", connectionId)
                return
            }
            vertx.eventBus().send(
                "${UpSocketVerticle.ADDRESS_SEND_TO_CONNECTION}.${deploymentId}",
                JsonObject()
                    .put("connectionId", connectionId)
                    .put("message", message)
            )
        } catch (e: Exception) {
            log.warn("Failed to send message to connection {}: {}", connectionId, e.message)
        }
    }
}