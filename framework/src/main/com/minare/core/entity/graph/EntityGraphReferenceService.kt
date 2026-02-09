package com.minare.core.entity.graph

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.application.config.FrameworkConfig
import com.minare.core.entity.annotations.Child
import com.minare.core.entity.annotations.Parent
import com.minare.core.entity.services.EntityInspector
import com.minare.core.storage.interfaces.EntityGraphStore
import com.minare.core.storage.interfaces.StateStore
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

@Singleton
class EntityGraphReferenceService @Inject constructor(
    private val entityInspector: EntityInspector,
    private val stateStore: StateStore,
    private val entityGraphStore: EntityGraphStore,
    private val frameworkConfig: FrameworkConfig
) {
    private val log = LoggerFactory.getLogger(EntityGraphReferenceService::class.java)

    /**
     * Remove all references to entityId from related entities.
     * Updates both Redis (state) and Mongo (graph) stores.
     */
    suspend fun removeReferencesToEntity(entityId: String) {
        val entityJson = stateStore.findEntityJson(entityId)
        if (entityJson == null) {
            log.warn("Cannot clean up references: entity {} not found", entityId)
            return
        }

        // 1. Collect all referenced entity IDs
        val referencedIds = collectReferencedEntityIds(entityId, entityJson)
        if (referencedIds.isEmpty()) return

        // 2. Batch fetch all referenced entities
        val referencedEntities = stateStore.findEntitiesJson(referencedIds)

        // 3. Build deltas for each entity that needs updating
        val deltas = buildRemovalDeltas(referencedEntities, entityId)
        if (deltas.isEmpty()) return

        // 4. Batch update Redis
        removeReferencesFromRedis(deltas)

        // 5. Batch update Mongo if enabled
        if (frameworkConfig.mongo.enabled) {
            removeReferencesFromMongo(deltas)
        }

        log.debug("Removed references to {} from {} entities", entityId, deltas.size)
    }

    /**
     * Collect all entity IDs referenced by this entity's relationship fields
     */
    private suspend fun collectReferencedEntityIds(entityId: String, entityJson: JsonObject): List<String> {
        val state = entityJson.getJsonObject("state") ?: return emptyList()
        val relationshipFields = entityInspector.getFieldsOfType(
            entityId,
            listOf(Parent::class, Child::class)
        )

        return relationshipFields.flatMap { field ->
            extractEntityIds(state.getValue(field.name))
        }.distinct()
    }

    /**
     * Build deltas for all entities that reference entityIdToRemove
     */
    private suspend fun buildRemovalDeltas(
        entities: Map<String, JsonObject>,
        entityIdToRemove: String
    ): Map<String, JsonObject> {
        val deltas = mutableMapOf<String, JsonObject>()

        for ((targetEntityId, targetJson) in entities) {
            val delta = buildRemovalDelta(targetEntityId, targetJson, entityIdToRemove)
            if (delta != null && !delta.isEmpty) {
                deltas[targetEntityId] = delta
            }
        }

        return deltas
    }

    /**
     * Build a delta that removes entityIdToRemove from a single entity's relationship fields
     */
    private suspend fun buildRemovalDelta(
        targetEntityId: String,
        targetJson: JsonObject,
        entityIdToRemove: String
    ): JsonObject? {
        val state = targetJson.getJsonObject("state") ?: return null
        val relationshipFields = entityInspector.getFieldsOfType(
            targetEntityId,
            listOf(Parent::class, Child::class)
        )

        val delta = JsonObject()

        for (field in relationshipFields) {
            val fieldValue = state.getValue(field.name) ?: continue
            val updatedValue = removeIdFromField(fieldValue, entityIdToRemove)

            if (updatedValue != fieldValue) {
                delta.put(field.name, updatedValue)
            }
        }

        return if (delta.isEmpty) null else delta
    }

    /**
     * Batch update Redis state store
     */
    private suspend fun removeReferencesFromRedis(deltas: Map<String, JsonObject>) {
        stateStore.batchSaveState(deltas)
    }

    /**
     * Batch update Mongo graph store
     */
    private suspend fun removeReferencesFromMongo(deltas: Map<String, JsonObject>) {
        entityGraphStore.bulkUpdateRelationships(deltas)
    }

    /**
     * Extract entity IDs from a field value (handles String, List, JsonArray)
     */
    private fun extractEntityIds(value: Any?): List<String> {
        return when (value) {
            null -> emptyList()
            is String -> if (value.isNotBlank()) listOf(value) else emptyList()
            is List<*> -> value.filterIsInstance<String>()
            is JsonArray -> value.list.filterIsInstance<String>()
            else -> {
                log.warn("Unexpected relationship field type: {}", value::class.simpleName)
                emptyList()
            }
        }
    }

    /**
     * Remove an ID from a field value, preserving the field's type
     */
    private fun removeIdFromField(value: Any, idToRemove: String): Any? {
        return when (value) {
            is String -> if (value == idToRemove) null else value
            is List<*> -> value.filter { it != idToRemove }
            is JsonArray -> JsonArray(value.list.filter { it != idToRemove })
            else -> value
        }
    }
}