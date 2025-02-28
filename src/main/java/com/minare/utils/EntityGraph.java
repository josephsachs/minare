package com.minare.utils;

import lombok.Getter;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.DepthFirstIterator;
import org.jgrapht.traverse.TopologicalOrderIterator;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import com.minare.core.models.Entity;
import com.minare.core.models.annotations.entity.State;

public class EntityGraph {
    @Getter
    private final SimpleDirectedGraph<Entity, DefaultEdge> graph;
    private final Set<Entity> visited;

    public EntityGraph(Entity root) {
        this.graph = new SimpleDirectedGraph<>(DefaultEdge.class);
        this.visited = new HashSet<>();
        buildGraph(root);
    }

    public TopologicalOrderIterator<Entity, DefaultEdge> getTopologicalIterator() {
        return new TopologicalOrderIterator<>(graph);
    }

    public DepthFirstIterator<Entity, DefaultEdge> getDepthFirstIterator() {
        return new DepthFirstIterator<>(graph);
    }

    /**
     * Recursively builds a directed graph from an entity hierarchy.
     * For each entity encountered:
     * 1. Adds it as a vertex if not already visited
     * 2. Processes its @StateField annotated fields
     * 3. Creates edges for entity references
     * 4. Handles both direct entity references and collections of Entities
     *
     * @param root The root entity from which to build the graph
     */
    private void buildGraph(Entity root) {
        if (visited.contains(root)) {
            return;
        }

        visited.add(root);
        graph.addVertex(root);

        // Get all @StateField annotated fields
        for (Field field : root.getClass().getDeclaredFields()) {
            if (field.isAnnotationPresent(State.class)) {
                field.setAccessible(true);
                try {
                    Object value = field.get(root);
                    addFieldToGraph(value, root);
                } catch (IllegalAccessException e) {
                    throw new RuntimeException("Failed to access field: " + field.getName(), e);
                }
            }
        }
    }

    /**
     * Processes a single field value, adding appropriate vertices and edges to the graph.
     * - For entity fields: adds vertex and edge, then recursively processes the entity
     * - For Collections: processes each entity element in the collection
     * - Ignores null values and non-entity fields
     *
     * @param value The field value to process
     * @param source The entity containing this field
     */
    private void addFieldToGraph(Object value, Entity source) {
        if (value == null) {
            return;
        }

        if (value instanceof Entity) {
            Entity target = (Entity) value;
            graph.addVertex(target);
            graph.addEdge(source, target);
            buildGraph(target);
        } else if (value instanceof Collection<?>) {
            for (Object item : (Collection<?>) value) {
                if (item instanceof Entity) {
                    addFieldToGraph(item, source);
                }
            }
        }
    }
}