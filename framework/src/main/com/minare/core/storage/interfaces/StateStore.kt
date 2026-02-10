package com.minare.core.storage.interfaces

import com.minare.core.entity.models.Entity
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge

/**
 * Interface for state storage operations.
 * Handles entity state persistence and basic queries needed for mutations.
 */
interface StateStore {
    /**
     * Saves an entity to the state store
     * @param entity The entity to save
     * @return The saved entity with ID and version set
     */
    suspend fun save(entity: Entity): Entity

    /**
     * Mutates an entity's state fields based on the provided delta
     * @param entityId The ID of the entity to update
     * @param delta The filtered delta containing only fields that passed consistency checks
     * @return The updated entity document
     */
    suspend fun saveState(entityId: String, delta: JsonObject, incrementVersion: Boolean = true): JsonObject

    /**
     * Batch updates state for multiple entities, incrementing versions
     * @param updates Map of entityId to delta JsonObject
     */
    suspend fun batchSaveState(updates: Map<String, JsonObject>)

    /**
     * Persists all of an entity's properties
     * @param entityId The ID of the entity to update
     * @param delta
     * @param publish Send pubsub notification
     * @return The updated entity document
     */
    suspend fun saveProperties(entityId: String, delta: JsonObject, publish: Boolean = false): JsonObject

    /**
     * Deletes an entity from the state store
     * @param entityId The ID of the entity to delete
     * @return true if entity was deleted, false if not found
     */
    suspend fun delete(entityId: String): Boolean

    /**
     * Find single entity by ID
     * @param entityId String
     * @return Entity?
     */
    suspend fun findOne(entityId: String): Entity?

    /**
     * Finds multiple entities by their IDs
     * @param entityIds List of entity IDs to fetch
     * @return Map of entity IDs to entity objects
     */
    suspend fun find(entityIds: List<String>): Map<String, Entity>

    /**
     * Populates all @State fields on an entity using reflection
     * @param entity The entity to populate
     * @param entityType The type name for reflection lookup
     * @param state The state JSON to apply
     * @return The entity with state fields populated
     */
    suspend fun setEntityState(entity: Entity, entityType: String, state: JsonObject): Entity

    /**
     * Populates all @Property fields on an entity using reflection
     * @param entity The entity to populate
     * @param entityType The type name for reflection lookup
     * @param properties The properties JSON to apply
     * @return The entity with property fields populated
     */
    suspend fun setEntityProperties(entity: Entity, entityType: String, properties: JsonObject): Entity

    /**
     * Hydrates a graph of JsonObject nodes with full entity state from Redis
     * @param graph Graph with nodes containing minimal entity info (id, type, version)
     * @return The same graph structure but with nodes containing full state
     */
    suspend fun hydrateGraph(graph: Graph<JsonObject, DefaultEdge>)

    /**
     * Finds an entity's type by ID
     * @param entityId The ID of the entity to fetch
     * @return The entity type, or null if not found
     */
    suspend fun findEntityType(entityId: String): String?

    /**
     * Finds all entity keys for a given entity type
     * @param type String
     * @return List<String>
     */
    suspend fun findAllKeysForType(type: String): List<String>

    /**
     * Finds multiple entities by their IDs and returns as JsonObjects
     * @param entityIds List of entity IDs to fetch
     * @return Map of entity IDs to JsonObject documents
     */
    suspend fun findJson(entityIds: List<String>): Map<String, JsonObject>

    /**
     * Finds an entity by ID and returns it as a JsonObject
     * @param entityId The ID of the entity to fetch
     * @return The entity as a JsonObject, or null if not found
     */
    suspend fun findOneJson(entityId: String): JsonObject?

    /**
     * Finds multiple entities by their IDs and returns them as JsonObjects
     * @param entityIds List of entity IDs to fetch
     * @return Map of entity IDs to JsonObjects
     */
    suspend fun findJsonByIds(entityIds: List<String>): Map<String, JsonObject>

    /**
     * Get all entity keys from the store
     * @return List of all entity IDs (excluding frame deltas and other non-entity keys)
     */
    suspend fun getAllEntityKeys(): List<String>
}