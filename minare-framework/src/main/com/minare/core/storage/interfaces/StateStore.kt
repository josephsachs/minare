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
    suspend fun mutateState(entityId: String, delta: JsonObject): JsonObject

    /**
     * Finds multiple entities by their IDs
     * @param entityIds List of entity IDs to fetch
     * @return Map of entity IDs to entity objects
     */
    suspend fun findEntitiesByIds(entityIds: List<String>): Map<String, Entity>

    /**
     * Finds multiple entities by their IDs
     * @param entityIds List of entity IDs to fetch
     * @return Map of entities with full state
     */
    suspend fun findEntitiesWithState(entityIds: List<String>): Map<String, Entity>

    /**
     * Hydrates a graph of JsonObject nodes with full entity state from Redis
     * @param graph Graph with nodes containing minimal entity info (id, type, version)
     * @return The same graph structure but with nodes containing full state
     */
    suspend fun hydrateGraph(graph: Graph<JsonObject, DefaultEdge>)

    /**
     * Finds an entity by ID and returns it as a JsonObject
     * @param entityId The ID of the entity to fetch
     * @return The entity as a JsonObject, or null if not found
     */
    suspend fun findEntityJson(entityId: String): JsonObject?

    /**
     * Finds multiple entities by their IDs and returns them as JsonObjects
     * @param entityIds List of entity IDs to fetch
     * @return Map of entity IDs to JsonObjects
     */
    suspend fun findEntitiesJsonByIds(entityIds: List<String>): Map<String, JsonObject>

    /**
     * Get all entity keys from the store
     * @return List of all entity IDs (excluding frame deltas and other non-entity keys)
     */
    suspend fun getAllEntityKeys(): List<String>
}