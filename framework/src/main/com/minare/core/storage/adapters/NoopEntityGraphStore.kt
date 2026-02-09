package com.minare.core.storage.adapters

import com.google.inject.Singleton
import com.minare.core.entity.models.Entity
import com.minare.core.storage.interfaces.EntityGraphStore
import io.vertx.core.json.JsonObject

/**
 * No-op implementation of EntityGraphStore for when MongoDB is disabled.
 * All operations succeed but do nothing.
 */
@Singleton
class NoopEntityGraphStore : EntityGraphStore {

    override suspend fun save(entity: Entity, create: Boolean): Entity {
        return entity
    }

    override suspend fun updateRelationships(entityId: String, delta: JsonObject): JsonObject {
        return JsonObject()
    }

    override suspend fun bulkUpdateRelationships(updates: Map<String, JsonObject>) {
        // No-op
    }

    override suspend fun updateVersions(entityIds: Set<String>): JsonObject {
        return JsonObject().put("modified", 0)
    }

    override suspend fun findEntitiesByIds(entityIds: List<String>): Map<String, Entity> {
        return emptyMap()
    }

    override suspend fun delete(entityId: String): Boolean {
        return true
    }
}