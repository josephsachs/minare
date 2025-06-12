package com.minare.worker.update.events

import com.google.inject.Inject
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.update.handlers.EntityUpdateHandler

class EntityUpdatedEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val entityUpdateHandler: EntityUpdateHandler,
    private val vlog: VerticleLogger
) {
    suspend fun register(deploymentId: String) {
        vlog.getEventLogger().trace("REGISTERING_ENTITY_UPDATE_HANDLER", mapOf(
            "deploymentId" to deploymentId
        ))

        // Set the deployment ID on the handler to establish ownership
        entityUpdateHandler.setDeploymentId(deploymentId)

        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_ENTITY_UPDATED) { message, traceId ->
            entityUpdateHandler.handle(message.body(), traceId)
        }

        vlog.logHandlerRegistration(ADDRESS_ENTITY_UPDATED)
    }

    companion object {
        const val ADDRESS_ENTITY_UPDATED = "minare.entity.update"
    }
}