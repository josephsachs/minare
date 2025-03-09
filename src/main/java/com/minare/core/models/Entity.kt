package com.minare.core.models

import com.fasterxml.jackson.annotation.*
import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.minare.core.entity.EntityFactory
import com.minare.core.entity.EntityReflector
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.serialize.EntitySerializationVisitor
import com.minare.persistence.EntityStore
import com.minare.utils.EntityGraph
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext

import io.vertx.kotlin.coroutines.await
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge
import org.jgrapht.traverse.DepthFirstIterator
import javax.inject.Inject

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonIgnoreProperties(ignoreUnknown = true)
open class Entity {
        @JsonProperty("version")
        var version: Int = 0

        @JsonProperty("_id")
        var _id: String? = null

        @JsonProperty("type")
        var type: String? = null

        @Inject
        lateinit var entityFactory: EntityFactory

        @Inject
        lateinit var entityStore: EntityStore

        @Inject
        lateinit var entityReflector: EntityReflector

        companion object {
                private val objectMapper = ObjectMapper()
                        .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
                        .setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY)
        }

        override fun equals(other: Any?): Boolean {
                if (this === other) return true
                if (other !is Entity) return false
                return _id == other._id
        }

        override fun hashCode(): Int {
                return _id?.hashCode() ?: 0
        }

        /**
         * Serializes this entity and its related entities into a JSON array.
         */
        suspend fun serialize(): JsonArray {
                return withContext(Dispatchers.Default) {
                        val graph = EntityGraph(this@Entity)
                        val visitor = EntitySerializationVisitor()

                        val iterator = graph.getDepthFirstIterator()
                        while (iterator.hasNext()) {
                                visitor.visit(iterator.next())
                        }

                        return@withContext visitor.documents
                }
        }

        /**
         * Updates the version of this entity and its ancestors based on parent reference rules.
         */
        suspend fun update(): JsonObject {
                if (!::entityStore.isInitialized) {
                        throw IllegalStateException("EntityStore not set")
                }

                if (_id == null) {
                        throw IllegalStateException("Entity ID not set")
                }

                // Get the ancestor graph
                val graph = entityStore.getAncestorGraph(_id!!)

                // Find entities that need version updates
                val idsToUpdate = findEntitiesForVersionUpdate(graph)

                // Update versions and return the result
                return entityStore.updateVersions(idsToUpdate)
        }

        /**
         * Finds entities that need version updates based on parent reference rules.
         */
        fun findEntitiesForVersionUpdate(graph: Graph<Entity, DefaultEdge>): Set<String> {
                val idsToUpdate = mutableSetOf<String>()
                val visited = mutableSetOf<String>()

                // Find self in the graph
                val self = graph.vertexSet()
                        .find { _id == it._id }
                        ?: return idsToUpdate

                // Always update self
                _id?.let { idsToUpdate.add(it) }

                // Use recursion to traverse the graph
                traverseParents(graph, self, idsToUpdate, visited)

                return idsToUpdate
        }

        /**
         * Traverses from an entity to its parents, collecting IDs that need version updates.
         */
        private fun traverseParents(
                graph: Graph<Entity, DefaultEdge>,
                entity: Entity,
                idsToUpdate: MutableSet<String>,
                visited: MutableSet<String>
        ) {
                // Mark as visited to prevent cycles
                entity._id?.let { visited.add(it) }

                // Get all outgoing edges (child to parent)
                val outEdges = graph.outgoingEdgesOf(entity)

                // Process each parent
                outEdges.forEach { edge ->
                        val parent = graph.getEdgeTarget(edge)

                        // Skip if already visited
                        if (parent._id in visited) {
                                return@forEach
                        }

                        // Check if we should bubble version to this parent
                        if (shouldBubbleVersionToParent(entity, parent)) {
                                // Add parent to the update set
                                parent._id?.let { idsToUpdate.add(it) }

                                // Continue traversal to this parent's parents
                                traverseParents(graph, parent, idsToUpdate, visited)
                        }
                        // If bubble_version is false, stop traversal along this path
                }
        }

        /**
         * Determines if version changes should bubble from a child entity to a parent entity.
         */
        private fun shouldBubbleVersionToParent(child: Entity, parent: Entity): Boolean {
                return try {
                        // Get reflection cache for the child entity type
                        val childCache = entityReflector.getReflectionCache(child::class.java)
                                ?: return false

                        // Check if any parent reference has bubble_version=false
                        // This is a simplified implementation - in a complete version,
                        // you would need to determine which specific field links to this particular parent
                        childCache.parentReferenceFields.all { it.isBubbleVersion }
                } catch (e: Exception) {
                        // If any error occurs, don't bubble
                        false
                }
        }
}