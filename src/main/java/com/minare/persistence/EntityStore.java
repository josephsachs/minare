package com.minare.persistence;

import com.minare.core.models.Entity;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.Set;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;

/**
 * Interface for entity storage operations.
 * This abstraction allows for different database implementations.
 */
public interface EntityStore {

    /**
     * Gets a graph of ancestors for an entity.
     * The graph contains Entity instances (with proper subtypes) but without full state.
     *
     * @param entityId The ID of the entity
     * @return A future containing a graph of entity objects representing the entity and its ancestors
     */
    Future<Graph<Entity, DefaultEdge>> getAncestorGraph(String entityId);

    /**
     * Updates the version of multiple entities.
     *
     * @param entityIds The set of entity IDs to update
     * @return A future with the update results
     */
    Future<JsonObject> updateVersions(Set<String> entityIds);
}