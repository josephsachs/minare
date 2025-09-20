package com.minare.controller

import com.minare.core.entity.models.Entity
import com.minare.core.storage.interfaces.EntityGraphStore
import com.minare.core.storage.interfaces.StateStore
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Controller for entity persistence operations.
 * Handles Redis-first storage with write-behind persistence to MongoDB.
 *
 * This follows the framework pattern:
 * - Framework provides this open base class
 * - Applications must bind it in their module (to this class or their extension)
 * - Applications can extend this class to customize behavior
 */
@Singleton
open class EntityController @Inject constructor(
    private val stateStore: StateStore,
    private val entityGraphStore: EntityGraphStore
) {
    private val log = LoggerFactory.getLogger(EntityController::class.java)

    /**
     * Create a new entity with proper two-phase lifecycle.
     *
     * This method handles the MongoDB-first-then-Redis flow for new entities:
     * 1. Save to MongoDB to get an ID assigned
     * 2. Save to Redis for fast state access
     *
     * @param entity The new entity to create (should not have an ID)
     * @return The created entity with ID assigned
     */
    open suspend fun create(entity: Entity): Entity {
        if (!entity._id.isNullOrEmpty()) {
            throw IllegalArgumentException("Entity already has an ID - use save() for downSockets")
        }

        try {
            //  Save to MongoDB to get ID assigned
            val entityWithId = entityGraphStore.save(entity)

            // Redis is source of truth for Entity state
            val finalEntity = stateStore.save(entityWithId)

            return finalEntity
        } catch (e: Exception) {
            log.error("EntityController failed to write with ${e}")
            throw e
        }
    }

    /**
     * Save an existing entity to Redis (source of truth) with write-behind to MongoDB.
     * Updated to use JsonObject-based WriteBehindStore for consistency.
     *
     * @param entity The entity to save (must already have an ID)
     * @return The saved entity with any updates
     */
    open suspend fun save(entityId: String?, deltas: JsonObject, incrementVersion: Boolean = true): Entity? {
        // TODO: Only save Entity relationships to MongoDB, ignore other state, but ensure tandem write

        if (entityId.isNullOrBlank()) {
            throw IllegalArgumentException("Entity must have an ID - use create() for new entities")
        }

        log.debug("Saving existing entity {} to Redis", entityId)

        stateStore.mutateState(entityId, deltas, incrementVersion)

        entityGraphStore.updateRelationships(entityId, deltas)

        return stateStore.findEntity(entityId)
    }

    /**
     * Find multiple entities by their IDs from Redis.
     *
     * @param ids List of entity IDs
     * @return Map of ID to Entity for found entities
     */
    open suspend fun findByIds(ids: List<String>): Map<String, Entity> {
        log.debug("Finding entities by IDs: {}", ids)
        return stateStore.findEntitiesByIds(ids)
    }

    /**
     * Find multiple entities with full state by their IDs from Redis.
     *
     * @param ids List of entity IDs
     * @return Map of ID to Entity for found entities with state populated
     */
    open suspend fun findByIdsWithState(ids: List<String>): Map<String, Entity> {
        log.debug("Finding entities with state by IDs: {}", ids)
        return stateStore.findEntitiesWithState(ids)
    }

    /**
     * Mutate an entity's state fields based on the provided delta.
     *
     * @param entityId The ID of the entity to update
     * @param delta The delta containing fields to update
     * @return The updated entity document
     */
    open suspend fun mutateState(entityId: String, delta: JsonObject): JsonObject {
        log.debug("Mutating state for entity {}", entityId)
        return stateStore.mutateState(entityId, delta)
    }
}