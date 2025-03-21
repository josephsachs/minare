package com.minare.utils

import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.SimpleDirectedGraph
import org.jgrapht.traverse.DepthFirstIterator
import org.jgrapht.traverse.TopologicalOrderIterator
import com.minare.core.models.Entity
import com.minare.core.entity.annotations.State
import com.minare.core.entity.ReflectionCache
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph
import kotlin.reflect.KClass

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
    suspend fun toJson(reflectionCache: ReflectionCache? = null): JsonObject {
        return graphToJson(graph, reflectionCache)
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
         * Converts a graph of document objects to a JsonObject suitable for sync responses
         *
         * @param graph The document graph to convert
         * @return JsonObject representation of the graph
         */
        suspend fun documentGraphToJson(graph: Graph<JsonObject, DefaultEdge>): JsonObject {
            val result = JsonObject()
            val entitiesArray = JsonArray()

            // Add each document to the entities array
            graph.vertexSet().forEach { document ->
                val entityJson = JsonObject()
                    .put("_id", document.getString("_id"))
                    .put("version", document.getLong("version", 1L))
                    .put("type", document.getString("type"))
                    .put("state", document.getJsonObject("state", JsonObject()))

                entitiesArray.add(entityJson)
            }

            // Add edges information
            val edges = JsonArray()
            graph.edgeSet().forEach { edge ->
                val source = graph.getEdgeSource(edge)
                val target = graph.getEdgeTarget(edge)

                edges.add(JsonObject()
                    .put("source", source.getString("_id"))
                    .put("target", target.getString("_id")))
            }

            return result
                .put("entities", entitiesArray)
                .put("edges", edges)
        }

        /**
         * Converts a graph of Entity objects to a JsonObject suitable for sync responses
         *
         * @param graph The entity graph to convert
         * @param reflectionCache Optional reflection cache to use for annotation lookup
         * @return JsonObject representation of the graph
         */
        suspend fun graphToJson(graph: Graph<Entity, DefaultEdge>, reflectionCache: ReflectionCache? = null): JsonObject {
            val result = JsonObject()
            val entitiesArray = JsonArray()

            // Add each entity to the array with its state fields
            for (entity in graph.vertexSet()) {
                // Extract state fields directly from the entity
                val stateJson = JsonObject()
                val entityType = entity.type

                if (entityType != null) {
                    val entityClass = entity.javaClass

                    // Get fields with @State annotation
                    val stateFields = if (reflectionCache != null) {
                        // Use reflection cache if provided
                        reflectionCache.getFieldsWithAnnotation<State>(entityClass.kotlin)
                    } else {
                        // Otherwise use direct reflection
                        entityClass.declaredFields.filter {
                            it.isAnnotationPresent(State::class.java)
                        }
                    }

                    for (field in stateFields) {
                        field.isAccessible = true
                        try {
                            val value = field.get(entity)
                            if (value != null) {
                                stateJson.put(field.name, value)
                            }
                        } catch (e: Exception) {
                            // Skip fields that can't be accessed
                        }
                    }
                }

                val entityJson = JsonObject()
                    .put("_id", entity._id)
                    .put("version", entity.version)
                    .put("type", entity.type)
                    .put("state", stateJson)

                entitiesArray.add(entityJson)
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
                .put("entities", entitiesArray)
                .put("edges", edges)
        }

        /**
         * Converts a collection of entities to a JsonObject without edge information
         *
         * @param entities Collection of Entity objects
         * @param reflectionCache Optional reflection cache to use for annotation lookup
         * @return JsonObject containing just the entities array
         */
        suspend fun entitiesToJson(
            entities: Collection<Entity>,
            reflectionCache: ReflectionCache? = null
        ): JsonObject {
            val result = JsonObject()
            val entitiesArray = JsonArray()

            for (entity in entities) {
                // Extract state fields directly from the entity
                val stateJson = JsonObject()
                val entityType = entity.type

                if (entityType != null) {
                    val entityClass = entity.javaClass

                    // Get fields with @State annotation
                    val stateFields = if (reflectionCache != null) {
                        // Use reflection cache if provided
                        reflectionCache.getFieldsWithAnnotation<State>(entityClass.kotlin)
                    } else {
                        // Otherwise use direct reflection
                        entityClass.declaredFields.filter {
                            it.isAnnotationPresent(State::class.java)
                        }
                    }

                    for (field in stateFields) {
                        field.isAccessible = true
                        try {
                            val value = field.get(entity)
                            if (value != null) {
                                stateJson.put(field.name, value)
                            }
                        } catch (e: Exception) {
                            // Skip fields that can't be accessed
                        }
                    }
                }

                val entityJson = JsonObject()
                    .put("_id", entity._id)
                    .put("version", entity.version)
                    .put("type", entity.type)
                    .put("state", stateJson)

                entitiesArray.add(entityJson)
            }

            return result.put("entities", entitiesArray)
        }

        /**
         * Converts a map of entities to a JsonObject without edge information
         *
         * @param entities Map of entity IDs to Entity objects
         * @param reflectionCache Optional reflection cache to use for annotation lookup
         * @return JsonObject containing just the entities array
         */
        suspend fun entitiesToJson(
            entities: Map<String, Entity>,
            reflectionCache: ReflectionCache? = null
        ): JsonObject {
            return entitiesToJson(entities.values, reflectionCache)
        }
    }
}