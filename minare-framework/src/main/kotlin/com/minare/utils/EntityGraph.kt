package com.minare.utils

import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.SimpleDirectedGraph
import org.jgrapht.traverse.DepthFirstIterator
import org.jgrapht.traverse.TopologicalOrderIterator
import com.minare.core.models.Entity
import com.minare.core.entity.annotations.State
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph

/**
 * Utility for building, traversing, and serializing entity relationship graphs.
 * This helps with operations that need to understand entity hierarchies.
 */
class EntityGraph(root: Entity) {

    val graph = SimpleDirectedGraph<Entity, DefaultEdge>(DefaultEdge::class.java)
    private val visited = mutableSetOf<Entity>()

    init {
        buildGraph(root)
    }

    /**
     * Returns an iterator that traverses the graph in topological order.
     * This ensures dependencies are processed before entities that depend on them.
     */
    fun getTopologicalIterator(): TopologicalOrderIterator<Entity, DefaultEdge> {
        return TopologicalOrderIterator(graph)
    }

    /**
     * Returns an iterator that traverses the graph in depth-first order.
     */
    fun getDepthFirstIterator(): DepthFirstIterator<Entity, DefaultEdge> {
        return DepthFirstIterator(graph)
    }

    /**
     * Converts this entity graph to a JsonObject suitable for sync responses
     *
     * @return JsonObject representation of the graph
     */
    suspend fun toJson(): JsonObject {
        return graphToJson(graph)
    }

    /**
     * Recursively builds a directed graph from an entity hierarchy.
     * For each entity encountered:
     * 1. Adds it as a vertex if not already visited
     * 2. Processes its @State annotated fields
     * 3. Creates edges for entity references
     * 4. Handles both direct entity references and collections of Entities
     *
     * @param root The root entity from which to build the graph
     */
    private fun buildGraph(root: Entity) {
        if (visited.contains(root)) {
            return
        }

        visited.add(root)
        graph.addVertex(root)

        // Get all @State annotated fields
        for (field in root.javaClass.declaredFields) {
            if (field.isAnnotationPresent(State::class.java)) {
                field.isAccessible = true
                try {
                    val value = field.get(root)
                    addFieldToGraph(value, root)
                } catch (e: IllegalAccessException) {
                    throw RuntimeException("Failed to access field: ${field.name}", e)
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
    private fun addFieldToGraph(value: Any?, source: Entity) {
        when {
            value == null -> return

            value is Entity -> {
                graph.addVertex(value)
                graph.addEdge(source, value)
                buildGraph(value)
            }

            value is Collection<*> -> {
                value.filterIsInstance<Entity>().forEach { entity ->
                    addFieldToGraph(entity, source)
                }
            }
        }
    }

    companion object {
        /**
         * Converts a graph of Entity objects to a JsonObject suitable for sync responses
         *
         * @param graph The entity graph to convert
         * @return JsonObject representation of the graph
         */
        suspend fun graphToJson(graph: Graph<Entity, DefaultEdge>): JsonObject {
            val result = JsonObject()
            val entities = JsonArray()

            // Add each entity to the array
            for (entity in graph.vertexSet()) {
                val entityJson = JsonObject()
                    .put("_id", entity._id)
                    .put("version", entity.version)
                    .put("state", entity.serialize())

                entities.add(entityJson)
            }

            // Add edges information
            val edges = JsonArray()
            for (edge in graph.edgeSet()) {
                val source = graph.getEdgeSource(edge)
                val target = graph.getEdgeTarget(edge)

                edges.add(JsonObject()
                    .put("source", source._id)
                    .put("target", target._id))
            }

            return result
                .put("entities", entities)
                .put("edges", edges)
        }

        /**
         * Converts a collection of entities to a JsonObject without edge information
         *
         * @param entities Collection of Entity objects
         * @return JsonObject containing just the entities array
         */
        suspend fun entitiesToJson(entities: Collection<Entity>): JsonObject {
            val result = JsonObject()
            val entitiesArray = JsonArray()

            for (entity in entities) {
                val entityJson = JsonObject()
                    .put("_id", entity._id)
                    .put("version", entity.version)
                    .put("state", entity.serialize())

                entitiesArray.add(entityJson)
            }

            return result.put("entities", entitiesArray)
        }

        /**
         * Converts a map of entities to a JsonObject without edge information
         *
         * @param entities Map of entity IDs to Entity objects
         * @return JsonObject containing just the entities array
         */
        suspend fun entitiesToJson(entities: Map<String, Entity>): JsonObject {
            return entitiesToJson(entities.values)
        }
    }
}