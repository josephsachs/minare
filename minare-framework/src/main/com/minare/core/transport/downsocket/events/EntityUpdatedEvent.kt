package com.minare.worker.downsocket.events

import com.google.inject.Inject
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import com.minare.core.transport.downsocket.RedisPubSubWorkerVerticle
import com.minare.worker.downsocket.handlers.EntityUpdateHandler
import io.vertx.core.json.JsonObject

/**
 * Event handler for entity update events.
 *
 * Handles both legacy individual updates and batched updates during transition.
 */
class EntityUpdatedEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val entityUpdateHandler: EntityUpdateHandler,
    private val vlog: VerticleLogger
) {
    suspend fun register() {
        // Register handler for legacy individual entity update events
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_ENTITY_UPDATED) { message, traceId ->
            // Only process individual updates if we're in transition period
            // TODO: Remove once all components use batched updates
            entityUpdateHandler.handle(message.body(), traceId)
        }
        vlog.logHandlerRegistration(ADDRESS_ENTITY_UPDATED)
    }

    companion object {
        const val ADDRESS_ENTITY_UPDATED = RedisPubSubWorkerVerticle.ADDRESS_ENTITY_UPDATED
    }
}