/**package com.minare.core.storage.interfaces

import com.minare.core.entity.models.Entity
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge

/**
 * Interface for entity persistence operations.
 * Implementations should handle entity storage, retrieval, and version management.
 */
interface EntityStore {
    /**
     * Builds a graph of document objects based on a list of entity IDs
     * @param entityIds List of entity IDs to include in the graph
     * @return A graph containing the raw documents as vertices and their relationships as edges
     */
    suspend fun buildDocumentGraph(entityIds: List<String>): Graph<JsonObject, DefaultEdge>


    /**
     * Saves an entity to the data store
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
     * Builds a graph of entities based on a list of entity IDs
     * @param entityIds List of entity IDs to include in the graph
     * @return A graph containing the requested entities and their relationships
     */
    suspend fun buildEntityGraph(entityIds: List<String>): Graph<Entity, DefaultEdge>

    /**
     * Gets the ancestor graph for a specific entity
     * @param entityId The ID of the entity to start with
     * @return A graph containing the entity and all its ancestors
     */
    suspend fun getAncestorGraph(entityId: String): Graph<Entity, DefaultEdge>

    /**
     * Updates the version numbers for multiple entities
     * @param entityIds Set of entity IDs to update versions for
     * @return JsonObject with information about the update results
     */
    suspend fun updateVersions(entityIds: Set<String>): JsonObject

}**/