package com.minare.controller

import com.minare.application.config.FrameworkConfig
import com.minare.core.entity.models.*
import com.minare.core.entity.services.EntityObjectHydrator
import com.minare.core.storage.interfaces.EntityGraphStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.debug.DebugLogger
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.storage.interfaces.EntityGraphStoreOption

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
open class EntityController @Inject constructor() {
    @Inject private lateinit var frameworkConfig: FrameworkConfig
    @Inject private lateinit var stateStore: StateStore
    @Inject private lateinit var entityGraphStore: EntityGraphStore
    @Inject private lateinit var objectHydrator: EntityObjectHydrator
    @Inject private lateinit var debug: DebugLogger

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
        try {
            val create = if (entity._id.endsWith("-unsaved")) {
                setId(entity)
                true
            } else {
                false
            }

            val newEntity = stateStore.save(entity)

            if (frameworkConfig.entity.graph.store in listOf(EntityGraphStoreOption.MONGO)) {
                entityGraphStore.save(entity, create)
            }

            return newEntity
        } catch (e: Exception) {
            log.error("EntityController failed to write with ${e}")
            throw e
        }
    }

    /**
     * Save an existing entity's state to Redis, tandem persisting relationship updates.
     * Use for @State fields.
     *
     * @param entity The entity to save (must already have an ID)
     * @return The saved entity with any updates
     */
    open suspend fun saveState(entityId: String, deltas: JsonObject, incrementVersion: Boolean = true): Entity? {
        debug.log(DebugLogger.Companion.DebugType.ENTITY_CONTROLLER_SAVE_ENTITY, listOf(entityId))

        stateStore.saveState(entityId, deltas, incrementVersion)

        if (frameworkConfig.entity.graph.store in listOf(EntityGraphStoreOption.MONGO)) {
            entityGraphStore.updateRelationships(entityId, deltas)
        }

        return stateStore.findOne(entityId)
    }

    /**
     * Save an existing entity's property to Redis.
     * Use for @Property fields.
     *
     * @param entity The entity to save (must already have an ID)
     * @return The saved entity with any updates
     */
    open suspend fun saveProperties(entityId: String, deltas: JsonObject): Entity? {
        stateStore.saveProperties(entityId, deltas)

        return stateStore.findOne(entityId)
    }

    /**
     * Delete an entity from all stores.
     *
     * @param entityId The ID of the entity to delete
     * @return true if successfully deleted
     */
    open suspend fun delete(entityId: String): Boolean {
        log.debug("Deleting entity {}", entityId)

        val deleted = stateStore.delete(entityId)

        if (frameworkConfig.entity.graph.store in listOf(EntityGraphStoreOption.MONGO)) {
            entityGraphStore.delete(entityId)
        }

        if (deleted) {
            log.debug("Entity {} deleted successfully", entityId)
        } else {
            log.warn("Entity {} not found for deletion", entityId)
        }

        return deleted
    }

    /**
     * Find multiple entities by their IDs from Redis.
     *
     * @param ids List of entity IDs
     * @return Map of ID to Entity for found entities
     */
    open suspend fun findByIds(ids: List<String>): Map<String, Entity> {
        val results = mutableMapOf<String, Entity>()
        val entityJsons = stateStore.findJson(ids)

        entityJsons.forEach { (entityKey, entityJson) ->
            results[entityKey] = objectHydrator.hydrate(entityJson)
        }

        return results
    }

    /**
     * Set the ID of the newly persisted entity. By default, strips
     * the suffix from the Java random UUID. Application can override
     * to set its own ID format if desired.
     */
    open suspend fun setId(entity: Entity) {
        entity._id = entity._id.removeSuffix("-unsaved")
    }
}