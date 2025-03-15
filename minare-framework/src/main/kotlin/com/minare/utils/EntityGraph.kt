package com.minare.utils

import org.jgrapht.graph.DefaultEdge
import org.jgrapht.graph.SimpleDirectedGraph
import org.jgrapht.traverse.DepthFirstIterator
import org.jgrapht.traverse.TopologicalOrderIterator
import com.minare.core.models.Entity
import com.minare.core.models.annotations.entity.State

/**
 * Utility for building and traversing a directed graph of entity relationships.
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
            if (field.isAnnotationPresent(_root_ide_package_.com.minare.core.models.annotations.entity.State::class.java)) {
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
}