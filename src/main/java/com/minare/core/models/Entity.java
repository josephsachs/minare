package com.minare.core.models;

import com.fasterxml.jackson.annotation.*;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minare.core.entity.EntityFactory;
import com.minare.core.entity.EntityReflector;
import com.minare.core.entity.ReflectionCache;
import com.minare.core.entity.serialize.EntitySerializationVisitor;
import com.minare.persistence.EntityStore;
import com.minare.utils.EntityGraph;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.traverse.DepthFirstIterator;

import javax.inject.Inject;
import java.util.*;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonIgnoreProperties(ignoreUnknown = true)
public class Entity {
    @JsonProperty("version")
    public int version;
    @JsonProperty("_id")
    public String _id;
    @JsonProperty("type")
    public String type;

    @Inject
    public EntityFactory entityFactory;
    @Inject
    public EntityStore entityStore;
    @Inject
    public EntityReflector entityReflector;

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
            .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

    public Future<JsonArray> serialize() {
        EntityGraph graph = new EntityGraph(this);
        EntitySerializationVisitor visitor = new EntitySerializationVisitor();

        DepthFirstIterator<Entity, DefaultEdge> iterator = graph.getDepthFirstIterator();
        while (iterator.hasNext()) {
            visitor.visit(iterator.next());
        }

        return Future.succeededFuture(visitor.getDocuments());
    }

    /**
     * Updates the version of this entity and its ancestors based on parent reference rules.
     *
     * @return A future that completes when the update is done
     */
    public Future<JsonObject> update() {
        if (entityStore == null) {
            return Future.failedFuture(new IllegalStateException("EntityStore not set"));
        }

        if (_id == null) {
            return Future.failedFuture(new IllegalStateException("Entity ID not set"));
        }

        // Get the ancestor graph
        return entityStore.getAncestorGraph(_id)
                .compose(graph -> {
                    // Find entities that need version updates
                    Set<String> idsToUpdate = findEntitiesForVersionUpdate(graph);

                    // Update versions
                    return entityStore.updateVersions(idsToUpdate);
                });
    }

    /**
     * Finds entities that need version updates based on parent reference rules.
     *
     * @param graph The graph of entities and their relationships
     * @return A set of entity IDs that need version updates
     */
    public Set<String> findEntitiesForVersionUpdate(Graph<Entity, DefaultEdge> graph) {
        Set<String> idsToUpdate = new HashSet<>();
        Set<String> visited = new HashSet<>();

        // Find self in the graph
        Entity self = graph.vertexSet().stream()
                .filter(v -> _id.equals(v._id))
                .findFirst()
                .orElse(null);

        if (self == null) {
            return idsToUpdate;
        }

        // Always update self
        idsToUpdate.add(_id);

        // Use recursion to traverse the graph
        traverseParents(graph, self, idsToUpdate, visited);

        return idsToUpdate;
    }

    /**
     * Traverses from an entity to its parents, collecting IDs that need version updates.
     *
     * @param graph The entity graph
     * @param entity The current entity
     * @param idsToUpdate Set to collect IDs that need updates
     * @param visited Set to track visited entities and prevent cycles
     */
    private void traverseParents(Graph<Entity, DefaultEdge> graph, Entity entity, Set<String> idsToUpdate, Set<String> visited) {
        // Mark as visited to prevent cycles
        visited.add(entity._id);

        // Get all outgoing edges (child to parent)
        Set<DefaultEdge> outEdges = graph.outgoingEdgesOf(entity);

        // Process each parent
        outEdges.forEach(edge -> {
            Entity parent = graph.getEdgeTarget(edge);

            // Skip if already visited
            if (visited.contains(parent._id)) {
                return;
            }

            // Check if we should bubble version to this parent
            if (shouldBubbleVersionToParent(entity, parent)) {
                // Add parent to the update set
                idsToUpdate.add(parent._id);

                // Continue traversal to this parent's parents
                traverseParents(graph, parent, idsToUpdate, visited);
            }
            // If bubble_version is false, stop traversal along this path
        });
    }

    /**
     * Determines if version changes should bubble from a child entity to a parent entity.
     *
     * @param child The child entity
     * @param parent The parent entity
     * @return True if version should bubble, false otherwise
     */
    private boolean shouldBubbleVersionToParent(Entity child, Entity parent) {
        try {
            // Get reflection cache for the child entity type
            ReflectionCache childCache = entityReflector.getReflectionCache(child.getClass());

            if (childCache == null) {
                return false;
            }

            // Check if any parent reference has bubble_version=false
            // This is a simplified implementation - in a complete version,
            // you would need to determine which specific field links to this particular parent
            return childCache.getParentReferenceFields().stream()
                    .allMatch(ReflectionCache.ParentReferenceField::isBubbleVersion);

        } catch (Exception e) {
            // If any error occurs, don't bubble
            return false;
        }
    }
}
