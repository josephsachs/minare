package com.minare.core.entity.services

import io.vertx.core.json.JsonObject

/**
 * Service for publishing entity state changes to channels.
 * Handles the messaging aspect of state mutations.
 */
interface EntityPublishService {
    /**
     * Publishes a state change event for an entity to all relevant channels.
     *
     * @param entityId The ID of the entity that changed
     * @param entityType The type of the entity
     * @param version The new version number after the change
     * @param delta The JsonObject containing only the fields that changed
     */
    suspend fun publishStateChange(entityId: String, entityType: String, version: Long, delta: JsonObject)
}