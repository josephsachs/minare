package com.minare.controller

import com.minare.core.models.Entity
import com.minare.persistence.StateStore
import com.minare.persistence.WriteBehindStore
import com.minare.persistence.EntityStore
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
    private val writeBehindStore: WriteBehindStore,
    private val entityStore: EntityStore
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
            throw IllegalArgumentException("Entity already has an ID - use save() for updates")
        }

        log.debug("Creating new entity of type {}", entity.type)

        // Phase 1: Save to MongoDB to get ID assigned
        val entityWithId = entityStore.save(entity)
        log.debug("Entity created in MongoDB with ID: {}", entityWithId._id)

        // Phase 2: Save to Redis for fast state access
        val finalEntity = stateStore.save(entityWithId)
        log.debug("Entity {} synced to Redis", finalEntity._id)

        return finalEntity
    }

    /**
     * Save an existing entity to Redis (source of truth) with write-behind to MongoDB.
     * Updated to use JsonObject-based WriteBehindStore for consistency.
     *
     * @param entity The entity to save (must already have an ID)
     * @return The saved entity with any updates
     */
    open suspend fun save(entity: Entity): Entity {
        if (entity._id.isNullOrEmpty()) {
            throw IllegalArgumentException("Entity must have an ID - use create() for new entities")
        }

        log.debug("Saving existing entity {} to Redis", entity._id)

        // Save to Redis first (source of truth for existing entities)
        val savedEntity = stateStore.save(entity)

        // Schedule write-behind to MongoDB using JsonObject approach
        try {
            // Get the entity as a JsonObject document from Redis
            val entityDocument = stateStore.findEntityJson(savedEntity._id!!)

            if (entityDocument != null) {
                writeBehindStore.persistForWriteBehind(entityDocument)
                log.debug("Entity {} written to MongoDB via write-behind", savedEntity._id)
            } else {
                log.warn("Could not retrieve entity document for write-behind: {}", savedEntity._id)
            }
        } catch (e: Exception) {
            log.warn("Write-behind failed for entity {}: {}", savedEntity._id, e.message)
            // Don't fail the operation - Redis is source of truth
        }

        return savedEntity
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