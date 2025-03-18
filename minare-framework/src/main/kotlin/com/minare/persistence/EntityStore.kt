package com.minare.persistence

import com.minare.core.models.Entity
import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge

/**
 * Interface for entity storage operations.
 * This abstraction allows for different database implementations.
 */
interface EntityStore {
    /**
     * Gets a graph of ancestors for an entity.
     * The graph contains Entity instances (with proper subtypes) but without full state.
     *
     * @param entityId The ID of the entity
     * @return A graph of entity objects representing the entity and its ancestors
     */
    suspend fun getAncestorGraph(entityId: String): Graph<Entity, DefaultEdge>

    /**
     * Updates the version of multiple entities.
     *
     * @param entityIds The set of entity IDs to update
     * @return The update results
     */
    suspend fun updateVersions(entityIds: Set<String>): JsonObject

    /**
     * Creates or updates an entity
     *
     * @param entity The entity to save
     * @return The saved entity
     */
    suspend fun save(entity: Entity): Entity

    /**
     * Execute mutate operation for delta
     *
     * @param entityId The entity ID to mutate
     * @param delta The changes to apply
     * @return The update results
     */
    suspend fun mutateState(entityId: String, delta: JsonObject): JsonObject

    /**
     * Fetches multiple entities by their IDs
     *
     * @param entityIds List of entity IDs to fetch
     * @return Map of entity IDs to entity objects
     */
    suspend fun findEntitiesByIds(entityIds: List<String>): Map<String, Entity>

    /**
     * Builds a graph of entities based on a list of entity IDs
     *
     * @param entityIds List of entity IDs to include in the graph
     * @return A graph containing the requested entities and their relationships
     */
    suspend fun buildEntityGraph(entityIds: List<String>): Graph<Entity, DefaultEdge>
}