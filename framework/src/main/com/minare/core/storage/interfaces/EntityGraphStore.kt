package com.minare.core.storage.interfaces

import com.minare.core.entity.models.Entity
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge

interface EntityGraphStore {
    /**
     * Saves an entity to the graph store
     * @param entity The entity to save
     * @param create Whether this is a new entity (insert vs update)
     * @return The saved entity
     */
    suspend fun save(entity: Entity, create: Boolean): Entity

    /**
     * Updates relationship fields for an entity
     * @param entityId The ID of the entity
     * @param delta The changes to apply
     * @return The update result
     */
    suspend fun updateRelationships(entityId: String, delta: JsonObject): JsonObject

    /**
     * Batch updates relationship fields for multiple entities
     * @param updates Map of entityId to delta JsonObject
     */
    suspend fun bulkUpdateRelationships(updates: Map<String, JsonObject>)

    /**
     * Updates versions for multiple entities
     * @param entityIds Set of entity IDs to update
     * @return The update result
     */
    suspend fun updateVersions(entityIds: Set<String>): JsonObject

    /**
     * Finds entities by their IDs
     * @param entityIds List of entity IDs to fetch
     * @return Map of entity ID to Entity
     */
    suspend fun findEntitiesByIds(entityIds: List<String>): Map<String, Entity>

    /**
     * Deletes an entity from the graph store
     * @param entityId The ID of the entity to delete
     * @return true if entity was deleted, false if not found
     */
    suspend fun delete(entityId: String): Boolean
}