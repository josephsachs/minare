package com.minare.persistence

import com.minare.core.models.Entity
import io.vertx.core.json.JsonObject

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
}