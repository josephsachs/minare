package com.minare.core.storage.adapters

import com.minare.core.entity.models.Entity
import com.minare.core.storage.interfaces.EntityGraphStore
import com.minare.exceptions.EntityStorageException
import io.vertx.core.json.JsonObject

/**
 * No-Op entity graph store for when no adapter is configured
 */
class NoopEntityGraphStore : EntityGraphStore {
    override suspend fun save(entity: Entity, create: Boolean): Entity {
        throw EntityStorageException("Attempted to store entity using No-Op entity graph store")
    }

    override suspend fun updateRelationships(entityId: String, delta: JsonObject): JsonObject {
        throw EntityStorageException("Attempted to update entity relationships using No-Op entity graph store")

    }

    override suspend fun updateVersions(entityIds: Set<String>): JsonObject {
        throw EntityStorageException("Attempted to update entity version data using No-Op entity graph store")

    }

    override suspend fun findEntitiesByIds(entityIds: List<String>): Map<String, Entity> {
        throw EntityStorageException("Attempted to find entities using No-Op entity graph store")
    }
}