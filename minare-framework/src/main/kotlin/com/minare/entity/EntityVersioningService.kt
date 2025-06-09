package com.minare.entity

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.entity.annotations.Parent
import com.minare.core.models.Entity
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge
import com.minare.persistence.EntityQueryStore
import com.minare.persistence.StateStore

@Singleton
class EntityVersioningService @Inject constructor(
    private val entityQueryStore: EntityQueryStore,
    private val stateStore: StateStore
) {
    /**
     * Updates the version of this entity and its ancestors based on parent reference rules.
     * Returns the result of the version update operation.
     */
    suspend fun bubbleVersions(entityId: String): JsonObject {
        val graph = entityQueryStore.getAncestorGraph(entityId)
        val idsToUpdate = findEntitiesForVersionUpdate(graph, entityId)
        return updateVersionsInRedis(idsToUpdate)
    }

    private fun findEntitiesForVersionUpdate(graph: Graph<Entity, DefaultEdge>, entityId: String): Set<String> {
        val idsToUpdate = mutableSetOf<String>()

        // Find the entity in the graph that matches our entityId
        val self = graph.vertexSet().find { it._id == entityId }
            ?: return idsToUpdate

        // Add the original entity's ID
        idsToUpdate.add(entityId)

        // Get all parents from the graph
        val allParents = Entity.traverseParents(graph, self)

        allParents.forEach { parent ->
            if (shouldBubbleVersionToParent(self, parent)) {
                parent._id?.let { idsToUpdate.add(it) }
            }
        }

        return idsToUpdate
    }

    /**
     * Determines if version changes should bubble from a child entity to a parent entity.
     */
    private fun shouldBubbleVersionToParent(child: Entity, parent: Entity): Boolean {
        return try {
            // Get all parent fields from the child entity
            val parentFields = child.getParentFields()

            if (parentFields.isEmpty()) {
                return false
            }

            val matchingParentFields = parentFields.filter { field ->
                field.isAccessible = true
                val fieldValue = field.get(child)

                // Check if this field references the parent we're checking
                when (fieldValue) {
                    is Entity -> fieldValue._id == parent._id
                    is String -> fieldValue == parent._id
                    else -> false
                }
            }

            if (matchingParentFields.isEmpty()) {
                return false
            }

            matchingParentFields.all { field ->
                field.getAnnotation(Parent::class.java)?.bubble_version ?: false
            }
        } catch (e: Exception) {
            false
        }
    }

    private suspend fun updateVersionsInRedis(idsToUpdate: Set<String>): JsonObject {
        if (idsToUpdate.isEmpty()) {
            return JsonObject().put("updatedCount", 0)
        }

        try {
            var updatedCount = 0

            for (entityId in idsToUpdate) {
                // Increment version in Redis using atomic operation
                val result = stateStore.mutateState(entityId, JsonObject().put("_versionIncrement", 1))
                if (result != null && result.containsKey("version")) {
                    updatedCount++
                }
            }

            return JsonObject()
                .put("updatedCount", updatedCount)
                .put("processedIds", idsToUpdate.size)
        } catch (e: Exception) {
            return JsonObject()
                .put("error", e.message)
                .put("updatedCount", 0)
        }
    }
}