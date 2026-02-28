package com.minare.core.transport.upsocket.events

import com.google.inject.Inject
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.transport.upsocket.UpSocketVerticle
import com.minare.core.utils.vertx.EventBusUtils
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

class EntitySyncEvent @Inject constructor(
    private val vertx: Vertx,
    private val eventBusUtils: EventBusUtils,
    private val connectionStore: ConnectionStore,
    private val stateStore: StateStore
) {
    private val log = LoggerFactory.getLogger(EntitySyncEvent::class.java)

    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(UpSocketVerticle.ADDRESS_ENTITY_SYNC) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            val entityId = message.body().getString("entityId")

            try {
                val result = handleEntitySync(connectionId, entityId)
                eventBusUtils.tracedReply(message, JsonObject().put("success", result), traceId)
            } catch (e: Exception) {
                log.error("Error handling entity sync for {}", connectionId, e)
                message.fail(500, e.message ?: "Error handling entity sync")
            }
        }
    }

    private suspend fun handleEntitySync(connectionId: String, entityId: String): Boolean {
        if (!connectionStore.exists(connectionId)) return false

        val entities = stateStore.findJsonByIds(listOf(entityId))
        val entity = entities[entityId] ?: return false

        val syncMessage = JsonObject()
            .put("type", "entity_sync")
            .put("data", entity.put("timestamp", System.currentTimeMillis()))

        sendToClient(connectionId, syncMessage)
        connectionStore.updateLastActivity(connectionId)
        return true
    }

    private suspend fun sendToClient(connectionId: String, message: JsonObject) {
        try {
            val connection = connectionStore.find(connectionId)
            val deploymentId = connection.upSocketDeploymentId ?: return
            vertx.eventBus().send(
                "${UpSocketVerticle.ADDRESS_SEND_TO_CONNECTION}.${deploymentId}",
                JsonObject()
                    .put("connectionId", connectionId)
                    .put("message", message)
            )
        } catch (e: Exception) {
            log.warn("Failed to send to connection {}: {}", connectionId, e.message)
        }
    }
}