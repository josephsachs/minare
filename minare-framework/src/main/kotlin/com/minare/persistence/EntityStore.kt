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
     * @param entityIds The set of entity IDs to update
     * @return The update results
     */
    suspend fun save(entity: Entity): Future<Entity>
}