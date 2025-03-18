package com.minare.core.models

import com.fasterxml.jackson.annotation.*
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.*
import com.minare.core.entity.serialize.EntitySerializationVisitor
import com.minare.persistence.EntityStore
import com.minare.utils.EntityGraph
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.*

import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge
import java.lang.reflect.Field
import javax.inject.Inject

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonIgnoreProperties(ignoreUnknown = true)
open class Entity {
        @JsonProperty("version")
        var version: Long = 1

        @JsonProperty("_id")
        var _id: String? = null

        @JsonProperty("type")
        var type: String? = null

        @Inject
        lateinit var entityStore: EntityStore

        @Inject
        lateinit var reflectionCache: ReflectionCache

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
         * Process the mutation based on consistency rules.
         */
        suspend fun mutate(requestObject: JsonObject): JsonObject {
                val requestEntity = requestObject.getJsonObject("entity")

                val delta = requestEntity.getJsonObject("state")
                val requestedVersion = requestEntity.getLong("version")
                val entityId = requestEntity.getString("_id")

                val prunedDelta = getMutateDelta(delta)

                if (prunedDelta.isEmpty) {
                        return JsonObject()
                                .put("success", false)
                                .put("message", "No valid mutable fields found")
                }

                val allowedChanges = filterDeltaByConsistencyLevel(prunedDelta, requestedVersion)

                if (allowedChanges.isEmpty) {
                        return JsonObject()
                                .put("success", false)
                                .put("message", "No allowed changes")
                }

                if (!::entityStore.isInitialized) {
                        throw IllegalStateException("EntityStore not set")
                }

                try {
                        // Perform the mutation and await the result
                        val result = entityStore.mutateState(entityId, allowedChanges)

                        // After mutation is complete, bubble versions
                        // Note: we're not waiting for bubbleVersions to complete before returning
                        // If you need to wait, use: val bubbleResult = bubbleVersions()
                        coroutineScope {
                                launch {
                                        bubbleVersions()
                                }
                        }
                        // Return success response with updated version
                        return JsonObject()
                                .put("success", true)
                                .put("version", result.getLong("version"))
                } catch (error: Exception) {
                        // Handle any errors
                        return JsonObject()
                                .put("success", false)
                                .put("message", "Mutation failed: ${error.message}")
                }
        }

        /**
         * Updates the version of this entity and its ancestors based on parent reference rules.
         * Returns the result of the version update operation.
         */
        suspend fun bubbleVersions(): JsonObject {
                if (!::entityStore.isInitialized) {
                        throw IllegalStateException("EntityStore not set")
                }

                if (_id == null) {
                        throw IllegalStateException("Entity ID not set")
                }

                // Get the ancestor graph using await extension function
                val graph = entityStore.getAncestorGraph(_id!!)

                // Find entities that need version updates
                val idsToUpdate = findEntitiesForVersionUpdate(graph)

                // Update the versions directly with the suspend function
                return entityStore.updateVersions(idsToUpdate)
        }

        /**
         * Finds entities that need version updates based on parent reference rules.
         */
        fun findEntitiesForVersionUpdate(graph: Graph<Entity, DefaultEdge>): Set<String> {
                val idsToUpdate = mutableSetOf<String>()
                val visited = mutableSetOf<String>()

                val self = graph.vertexSet()
                        .find { _id == it._id }
                        ?: return idsToUpdate

                _id?.let { idsToUpdate.add(it) }

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
                entity._id?.let { visited.add(it) }

                val outEdges = graph.outgoingEdgesOf(entity)

                outEdges.forEach { edge ->
                        val parent = graph.getEdgeTarget(edge)

                        if (parent._id in visited) {
                                return@forEach
                        }

                        if (shouldBubbleVersionToParent(entity, parent)) {
                                parent._id?.let { idsToUpdate.add(it) }

                                traverseParents(graph, parent, idsToUpdate, visited)
                        }
                }
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

        /**
         * Returns a filtered JsonObject containing only the fields that are mutable
         * based on the @Mutable annotation.
         */
        suspend fun getMutateDelta(delta: JsonObject): JsonObject {
                if (!::reflectionCache.isInitialized) {
                        throw IllegalStateException("ReflectionCache not initialized")
                }

                val mutableFields = reflectionCache.getFieldsWithAnnotation<Mutable>(this::class)

                return delta.fieldNames()
                        .asSequence()
                        .mapNotNull { fieldName ->
                                val field = mutableFields.find {
                                        val stateName = it.getAnnotation(State::class.java)?.fieldName?.takeIf { name -> name.isNotEmpty() } ?: it.name
                                        stateName == fieldName
                                } ?: return@mapNotNull null

                                val fieldValue = delta.getValue(fieldName)
                                val isTypeCompatible = when (field.type) {
                                        Int::class.java, Integer::class.java -> fieldValue is Int
                                        Long::class.java -> fieldValue is Long
                                        Double::class.java -> fieldValue is Double
                                        Float::class.java -> fieldValue is Float
                                        Boolean::class.java -> fieldValue is Boolean
                                        String::class.java -> fieldValue is String
                                        // Add more type checks as needed
                                        else -> true // For complex types, we'll let it pass for now
                                }

                                if (isTypeCompatible) Pair(fieldName, fieldValue) else null
                        }
                        .fold(JsonObject()) { acc, (fieldName, fieldValue) ->
                                acc.put(fieldName, fieldValue)
                        }
        }

        suspend fun filterDeltaByConsistencyLevel(delta: JsonObject, requestedVersion: Long): JsonObject {
                if (!::reflectionCache.isInitialized) {
                        throw IllegalStateException("ReflectionCache not initialized")
                }

                val mutableFields = getMutableFields()

                // First check for any STRICT fields with version mismatch
                val strictViolation = delta.fieldNames().any { fieldName ->
                        val field = findFieldByStateName(mutableFields, fieldName) ?: return@any false
                        val mutableAnnotation = field.getAnnotation(Mutable::class.java)

                        mutableAnnotation.consistency == ConsistencyLevel.STRICT && this.version != requestedVersion
                }

                if (strictViolation) {
                        // Log the violation
                        // logger.warn("Strict consistency violation detected. Entity: $id, Current: ${this.version}, Requested: $requestedVersion")
                        // Return empty JsonObject to indicate no fields should be updated
                        return JsonObject()
                }

                // Filter remaining fields based on consistency levels
                return delta.fieldNames()
                        .asSequence()
                        .mapNotNull { fieldName ->
                                val field = findFieldByStateName(mutableFields, fieldName) ?: return@mapNotNull null
                                val mutableAnnotation = field.getAnnotation(Mutable::class.java)

                                when (mutableAnnotation.consistency) {
                                        ConsistencyLevel.OPTIMISTIC -> Pair(fieldName, delta.getValue(fieldName))
                                        ConsistencyLevel.PESSIMISTIC -> {
                                                if (requestedVersion >= this.version) {
                                                        Pair(fieldName, delta.getValue(fieldName))
                                                } else null
                                        }
                                        ConsistencyLevel.STRICT -> Pair(fieldName, delta.getValue(fieldName)) // Already checked above
                                }
                        }
                        .fold(JsonObject()) { acc, (fieldName, fieldValue) ->
                                acc.put(fieldName, fieldValue)
                        }
        }

        // Helper function to find a field by its state name
        private fun findFieldByStateName(fields: List<Field>, stateName: String): Field? {
                return fields.find {
                        val stateAnnotation = it.getAnnotation(State::class.java)
                        val fieldStateName = stateAnnotation?.fieldName?.takeIf { name -> name.isNotEmpty() } ?: it.name
                        fieldStateName == stateName
                }
        }

        /**
         * Gets all fields marked with @Mutable annotation.
         */
        fun getMutableFields(): List<Field> {
                if (!::reflectionCache.isInitialized) {
                        throw IllegalStateException("ReflectionCache not initialized")
                }

                return reflectionCache.getFieldsWithAnnotation<Mutable>(this::class)
        }

        /**
         * Gets all fields marked with @Parent annotation.
         */
        fun getParentFields(): List<Field> {
                if (!::reflectionCache.isInitialized) {
                        throw IllegalStateException("ReflectionCache not initialized")
                }

                return reflectionCache.getFieldsWithAnnotation<Parent>(this::class)
        }

        /**
         * Gets all fields marked with @Child annotation.
         */
        fun getChildFields(): List<Field> {
                if (!::reflectionCache.isInitialized) {
                        throw IllegalStateException("ReflectionCache not initialized")
                }

                return reflectionCache.getFieldsWithAnnotation<Child>(this::class)
        }

        /**
         * Gets consistency level for a specific field.
         */
        fun getConsistencyLevel(fieldName: String): ConsistencyLevel? {
                val mutableFields = getMutableFields()

                val field = mutableFields.find {
                        val stateName = it.getAnnotation(State::class.java)?.fieldName?.takeIf { name -> name.isNotEmpty() } ?: it.name
                        stateName == fieldName
                }

                return field?.getAnnotation(Mutable::class.java)?.consistency
        }
}