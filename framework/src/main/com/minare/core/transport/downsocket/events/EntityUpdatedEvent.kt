package com.minare.core.transport.downsocket.events

import com.google.inject.Inject
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.transport.downsocket.RedisPubSubWorkerVerticle
import com.minare.core.transport.downsocket.handlers.EntityUpdateHandler
import io.vertx.core.json.JsonObject

/**
 * Event handler for individual entity update events published by RedisPubSubWorkerVerticle.
 * Routes each update to the correct DownSocketVerticle instance via EntityUpdateHandler.
 */
class EntityUpdatedEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val entityUpdateHandler: EntityUpdateHandler,
    private val vlog: VerticleLogger
) {
    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_ENTITY_UPDATED) { message, traceId ->
            entityUpdateHandler.handle(message.body(), traceId)
        }
        vlog.logHandlerRegistration(ADDRESS_ENTITY_UPDATED)
    }

    companion object {
        const val ADDRESS_ENTITY_UPDATED = RedisPubSubWorkerVerticle.ADDRESS_ENTITY_UPDATED
    }
}