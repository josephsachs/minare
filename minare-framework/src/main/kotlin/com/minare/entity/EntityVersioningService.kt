package com.minare.entity

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.entity.annotations.Parent
import com.minare.core.entity.EntityFactory
import com.minare.core.entity.ReflectionCache
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge
import com.minare.persistence.EntityQueryStore
import com.minare.persistence.StateStore

/**
 * Updated to work with JsonObject documents instead of Entity instances,
 * using direct ReflectionCache calls instead of Entity wrapper methods.
 */
@Singleton
class EntityVersioningService @Inject constructor(
    private val entityQueryStore: EntityQueryStore,
    private val stateStore: StateStore,
    private val reflectionCache: ReflectionCache,
    private val entityFactory: EntityFactory
) {
    /**
     * Updates the version of this entity and its ancestors based on parent reference rules.
     * Returns the result of the version update operation.
     *
     * Updated to use document graph instead of entity graph to avoid unnecessary Entity instantiation.
     */
    suspend fun bubbleVersions(entityId: String): JsonObject {
        // Use document graph instead of entity graph to avoid creating Entity instances
        val documentGraph = entityQueryStore.buildDocumentGraph(listOf(entityId))
        val idsToUpdate = findEntitiesForVersionUpdate(documentGraph, entityId)
        return updateVersionsInRedis(idsToUpdate)
    }

    /**
     * Find entities that need version updates by traversing the document graph.
     * Updated to work with JsonObject documents instead of Entity instances.
     */
    private fun findEntitiesForVersionUpdate(graph: Graph<JsonObject, DefaultEdge>, entityId: String): Set<String> {
        val idsToUpdate = mutableSetOf<String>()

        // Find the document in the graph that matches our entityId
        val selfDocument = graph.vertexSet().find { it.getString("_id") == entityId }
            ?: return idsToUpdate

        // Add the original entity's ID
        idsToUpdate.add(entityId)

        // Get all parent documents from the graph using JsonObject-based traversal
        val allParentDocuments = entityQueryStore.traverseParents(graph, selfDocument)

        allParentDocuments.forEach { parentDocument ->
            if (shouldBubbleVersionToParent(selfDocument, parentDocument)) {
                parentDocument.getString("_id")?.let { idsToUpdate.add(it) }
            }
        }

        return idsToUpdate
    }

    /**
     * Determines if version changes should bubble from a child document to a parent document.
     * Updated to work with JsonObject documents and use direct ReflectionCache calls.
     */
    private fun shouldBubbleVersionToParent(childDocument: JsonObject, parentDocument: JsonObject): Boolean {
        return try {
            val childType = childDocument.getString("type") ?: return false
            val parentId = parentDocument.getString("_id") ?: return false
            val childState = childDocument.getJsonObject("state", JsonObject())

            // Get the entity class for the child type using EntityFactory
            val childEntityClass = entityFactory.useClass(childType) ?: return false

            // Use direct ReflectionCache call instead of Entity wrapper method
            val parentFields = reflectionCache.getFieldsWithAnnotation<Parent>(childEntityClass.kotlin)

            if (parentFields.isEmpty()) {
                return false
            }

            val matchingParentFields = parentFields.filter { field ->
                val stateName = field.getAnnotation(com.minare.core.entity.annotations.State::class.java)
                    ?.fieldName?.takeIf { it.isNotEmpty() } ?: field.name

                val fieldValue = childState.getValue(stateName)

                // Check if this field references the parent we're checking
                when (fieldValue) {
                    is String -> fieldValue == parentId
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

    /**
     * Update versions in Redis for the specified entity IDs
     */
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