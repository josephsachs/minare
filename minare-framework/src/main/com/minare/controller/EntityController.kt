package com.minare.controller

import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.models.*
import com.minare.core.storage.interfaces.EntityGraphStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.debug.DebugLogger
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
open class EntityController @Inject constructor() {
    @Inject private lateinit var stateStore: StateStore
    @Inject private lateinit var entityGraphStore: EntityGraphStore
    @Inject private lateinit var entityFactory: EntityFactory
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
            //  Save to graph store to get ID assigned
            val entityWithId = entityGraphStore.save(entity)
            val finalEntity = stateStore.save(entityWithId)

            return finalEntity
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

        entityGraphStore.updateRelationships(entityId, deltas)

        return stateStore.findEntity(entityId)
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

        return stateStore.findEntity(entityId)
    }

    /**
     * Find multiple entities by their IDs from Redis.
     *
     * @param ids List of entity IDs
     * @return Map of ID to Entity for found entities
     */
    open suspend fun findByIds(ids: List<String>): Map<String, Entity> {
        var results = mutableMapOf<String, Entity>()
        val entityJsons = stateStore.findEntitiesJson(ids)

        entityJsons.forEach { (entityKey, entityJson) ->
            val entityType = entityJson.getString("type")
            val entityClass = entityFactory.useClass(entityType) ?: return@forEach

            val entity = entityFactory.createEntity(entityClass).apply {
                _id = entityJson.getString("_id")
                version = entityJson.getLong("version")
                type = entityType
            }

            val stateJson = entityJson.getJsonObject("state", JsonObject())
            stateStore.setEntityState(entity, entityType, stateJson)

            val propertiesJson = entityJson.getJsonObject("properties", JsonObject())
            stateStore.setEntityProperties(entity, entityType, propertiesJson)

            results[entityKey] = entity
        }

        return results
    }
}