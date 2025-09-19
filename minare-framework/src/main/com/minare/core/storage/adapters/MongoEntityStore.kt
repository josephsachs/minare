package com.minare.core.storage.adapters

import com.google.inject.Singleton
import com.minare.core.entity.models.Entity
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Parent
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.BulkOperation
import io.vertx.ext.mongo.MongoClient
import io.vertx.ext.mongo.WriteOption
import io.vertx.kotlin.coroutines.await
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge
import org.slf4j.LoggerFactory
import javax.inject.Inject
import com.minare.core.storage.interfaces.EntityQueryStore
import com.minare.core.storage.interfaces.EntityGraphStore

/**
 * MongoDB implementation of the EntityStore interface.
 * Handles entity relationship storage and graph operations.
 * Entity state is stored in Redis; this store only tracks relationships.
 */
@Singleton
class MongoEntityStore @Inject constructor(
    private val mongoClient: MongoClient,
    private val entityFactory: EntityFactory,
    private val reflectionCache: ReflectionCache
) : EntityGraphStore, EntityQueryStore {

    companion object {
        private const val COLLECTION_NAME = "entity_graph"
        private const val MAX_GRAPH_DEPTH = 10
    }

    private val log = LoggerFactory.getLogger(MongoEntityStore::class.java)

    /**
     * Creates a new entity with its relationships in the graph.
     * Only stores relationship fields, not full state.
     */
    override suspend fun save(entity: Entity): Entity {
        require(!entity.type.isNullOrBlank()) { "Entity type must be specified" }

        log.debug("Saving entity relationships for type: ${entity.type}")

        val document = buildEntityDocument(entity)

        try {
            if (entity._id.isNullOrEmpty()) {
                // New entity - let MongoDB generate an ID
                // Add version for new documents
                document.put("version", 1)
                val generatedId = mongoClient.insertWithOptions(
                    COLLECTION_NAME,
                    document,
                    WriteOption.ACKNOWLEDGED
                ).await()
                entity._id = generatedId
                entity.version = 1
                log.debug("Created new entity with ID: ${entity._id}")
            } else {
                // Existing entity - update relationships
                val query = JsonObject().put("_id", entity._id)
                val update = JsonObject().put("\$set", document)
                    .put("\$inc", JsonObject().put("version", 1))

                val result = mongoClient.findOneAndUpdateWithOptions(
                    COLLECTION_NAME,
                    query,
                    update,
                    io.vertx.ext.mongo.FindOptions(),
                    io.vertx.ext.mongo.UpdateOptions().setReturningNewDocument(true)
                ).await()

                if (result == null) {
                    throw IllegalStateException("Entity not found: ${entity._id}")
                }

                entity.version = result.getLong("version", 1)
                log.debug("Updated entity relationships: ${entity._id}")
            }
            return entity
        } catch (err: Exception) {
            log.error("Failed to save entity: ${entity._id}", err)
            throw err
        }
    }

    /**
     * Updates only the relationship fields in an entity's state.
     * Filters the delta to only include relationship-annotated fields.
     */
    override suspend fun updateRelationships(entityId: String, delta: JsonObject): JsonObject {
        if (delta.isEmpty) {
            return JsonObject()
        }

        val relationshipFields = getRelationshipFieldNames()
        val filteredDelta = delta.fieldNames()
            .filter { it in relationshipFields }
            .fold(JsonObject()) { acc, field ->
                acc.put("state.$field", delta.getValue(field))
            }

        if (filteredDelta.isEmpty) {
            log.debug("No relationship fields to update for entity: $entityId")
            return JsonObject()
        }

        val update = JsonObject()
            .put("\$set", filteredDelta)
            .put("\$inc", JsonObject().put("version", 1))

        val query = JsonObject().put("_id", entityId)

        try {
            val result = mongoClient.findOneAndUpdateWithOptions(
                COLLECTION_NAME,
                query,
                update,
                io.vertx.ext.mongo.FindOptions(),
                io.vertx.ext.mongo.UpdateOptions()
                    .setReturningNewDocument(true)
                    .setWriteOption(WriteOption.ACKNOWLEDGED)
            ).await()

            if (result == null) {
                throw IllegalStateException("Entity not found: $entityId")
            }

            log.debug("Updated entity relationships: $entityId")
            return result
        } catch (err: Exception) {
            log.error("Failed to update entity relationships: $entityId", err)
            throw err
        }
    }

    /**
     * Bulk updates versions for multiple entities with write concern.
     */
    override suspend fun updateVersions(entityIds: Set<String>): JsonObject {
        if (entityIds.isEmpty()) {
            return JsonObject().put("updatedCount", 0).put("matchedCount", 0)
        }

        val operations = entityIds.map { id ->
            BulkOperation.createUpdate(
                JsonObject().put("_id", id),
                JsonObject().put("\$inc", JsonObject().put("version", 1))
            )
        }

        val result = mongoClient.bulkWriteWithOptions(
            COLLECTION_NAME,
            operations,
            io.vertx.ext.mongo.BulkWriteOptions().setWriteOption(WriteOption.ACKNOWLEDGED)
        ).await()

        return JsonObject()
            .put("updatedCount", result.modifiedCount)
            .put("matchedCount", result.matchedCount)
    }

    /**
     * Fetches entities by their IDs. Returns minimal entity objects with ID, type, and version.
     * Full state should be hydrated from Redis if needed.
     */
    override suspend fun findEntitiesByIds(entityIds: List<String>): Map<String, Entity> {
        if (entityIds.isEmpty()) {
            return emptyMap()
        }

        try {
            val query = JsonObject().put(
                "\$or",
                JsonArray(entityIds.map { JsonObject().put("_id", it) })
            )

            val results = mongoClient.find(COLLECTION_NAME, query).await()

            return results.mapNotNull { document ->
                try {
                    val entity = createMinimalEntity(document)
                    entity._id?.let { id -> id to entity }
                } catch (e: Exception) {
                    log.error("Error creating entity from document: ${document.getString("_id")}", e)
                    null
                }
            }.toMap()
        } catch (err: Exception) {
            log.error("Failed to fetch entities by IDs", err)
            throw err
        }
    }

    /**
     * Retrieves all ancestors of an entity using MongoDB's $graphLookup.
     */
    override suspend fun getAncestorGraph(entityId: String): Graph<Entity, DefaultEdge> {
        val pipeline = JsonArray().apply {
            // Match the starting entity
            add(JsonObject().put("\$match", JsonObject().put("_id", entityId)))

            // Graph lookup for ancestors
            add(JsonObject().put("\$graphLookup", JsonObject().apply {
                put("from", COLLECTION_NAME)
                put("startWith", "\$_id")
                put("connectFromField", "_id")
                put("connectToField", "state.*")
                put("as", "ancestors")
                put("maxDepth", MAX_GRAPH_DEPTH)
                put("depthField", "depth")
            }))
        }

        val results = executeAggregation(pipeline)
        return transformResultsToEntityGraph(results)
    }

    /**
     * Builds a graph of entities based on their relationships.
     */
    override suspend fun buildEntityGraph(entityIds: List<String>): Graph<Entity, DefaultEdge> {
        if (entityIds.isEmpty()) {
            return DefaultDirectedGraph(DefaultEdge::class.java)
        }

        val documents = fetchDocumentsByIds(entityIds)
        return buildGraphFromDocuments(documents) { createMinimalEntity(it) }
    }

    /**
     * Builds a graph of raw MongoDB documents.
     */
    override suspend fun buildDocumentGraph(entityIds: List<String>): Graph<JsonObject, DefaultEdge> {
        if (entityIds.isEmpty()) {
            return DefaultDirectedGraph(DefaultEdge::class.java)
        }

        val documents = fetchDocumentsByIds(entityIds)
        return buildGraphFromDocuments(documents) { it }
    }

    /**
     * Traverses parent relationships in a document graph.
     */
    override fun traverseParents(
        graph: Graph<JsonObject, DefaultEdge>,
        document: JsonObject,
        visited: MutableSet<String>
    ): List<JsonObject> {
        val documentId = document.getString("_id") ?: return emptyList()
        visited.add(documentId)

        return graph.outgoingEdgesOf(document).flatMap { edge ->
            val parent = graph.getEdgeTarget(edge)
            val parentId = parent.getString("_id")

            if (parentId != null && parentId !in visited) {
                listOf(parent) + traverseParents(graph, parent, visited)
            } else {
                emptyList()
            }
        }
    }

    // Private helper methods

    private suspend fun buildEntityDocument(entity: Entity): JsonObject {
        val document = JsonObject()
            .put("type", entity.type)

        // Extract only relationship fields
        val relationshipState = extractRelationshipFields(entity)
        // Always include state field, even if empty
        document.put("state", relationshipState)

        return document
    }

    private fun extractRelationshipFields(entity: Entity): JsonObject {
        val stateJson = JsonObject()
        val entityType = entity.type ?: return stateJson

        entityFactory.useClass(entityType)?.let { entityClass ->
            // Get both Parent and Child relationship fields
            val parentFields = reflectionCache.getFieldsWithAnnotation<Parent>(entityClass)
            val childFields = reflectionCache.getFieldsWithAnnotation<com.minare.core.entity.annotations.Child>(entityClass)

            // Extract all relationship fields
            (parentFields + childFields).forEach { field ->
                field.isAccessible = true
                try {
                    val value = field.get(entity)
                    // Include the field even if null to maintain consistent schema
                    stateJson.put(field.name, value)
                } catch (e: Exception) {
                    log.warn("Error getting field ${field.name} from entity", e)
                }
            }
        }

        return stateJson
    }

    private fun getRelationshipFieldNames(): Set<String> {
        return entityFactory.getTypeNames().flatMap { typeName ->
            entityFactory.useClass(typeName)?.let { entityClass ->
                val parentFields = reflectionCache.getFieldsWithAnnotation<Parent>(entityClass)
                    .map { it.name }
                val childFields = reflectionCache.getFieldsWithAnnotation<com.minare.core.entity.annotations.Child>(entityClass)
                    .map { it.name }
                parentFields + childFields
            } ?: emptyList()
        }.toSet()
    }

    private fun createMinimalEntity(document: JsonObject): Entity {
        val type = document.getString("type") ?: "unknown"
        return entityFactory.getNew(type).apply {
            _id = document.getString("_id")
            version = document.getLong("version", 1L)
            this.type = type
        }
    }

    private suspend fun fetchDocumentsByIds(entityIds: List<String>): List<JsonObject> {
        val query = JsonObject().put(
            "\$or",
            JsonArray(entityIds.map { JsonObject().put("_id", it) })
        )

        return mongoClient.find(COLLECTION_NAME, query).await()
    }

    private suspend fun executeAggregation(pipeline: JsonArray): JsonArray {
        val results = JsonArray()
        val promise = io.vertx.core.Promise.promise<JsonArray>()

        mongoClient.aggregate(COLLECTION_NAME, pipeline)
            .handler { results.add(it) }
            .endHandler { promise.complete(results) }
            .exceptionHandler { promise.fail(it) }

        return promise.future().await()
    }

    private fun <T> buildGraphFromDocuments(
        documents: List<JsonObject>,
        nodeFactory: (JsonObject) -> T
    ): Graph<T, DefaultEdge> {
        val graph = DefaultDirectedGraph<T, DefaultEdge>(DefaultEdge::class.java)

        if (documents.isEmpty()) {
            return graph
        }

        // Create nodes and build lookup map
        val nodesById = documents.associate { document ->
            val node = nodeFactory(document)
            graph.addVertex(node)
            document.getString("_id") to (node to document)
        }

        // Add edges based on relationships
        nodesById.forEach { (sourceId, nodePair) ->
            val (sourceNode, document) = nodePair
            val state = document.getJsonObject("state", JsonObject())

            // Look for parent relationships
            state.fieldNames().forEach { fieldName ->
                val fieldValue = state.getValue(fieldName)
                extractEntityId(fieldValue)?.let { targetId ->
                    nodesById[targetId]?.let { (targetNode, _) ->
                        graph.addEdge(sourceNode, targetNode)
                    }
                }
            }
        }

        return graph
    }

    private fun extractEntityId(fieldValue: Any?): String? = when (fieldValue) {
        is String -> fieldValue
        is JsonObject -> fieldValue.getString("\$id") ?: fieldValue.getString("_id")
        else -> null
    }

    private fun transformResultsToEntityGraph(results: JsonArray): Graph<Entity, DefaultEdge> {
        val graph = DefaultDirectedGraph<Entity, DefaultEdge>(DefaultEdge::class.java)

        if (results.isEmpty) {
            return graph
        }

        // Flatten all entities from results
        val allDocuments = results.flatMap { result ->
            val rootDoc = result as JsonObject
            val ancestors = rootDoc.getJsonArray("ancestors", JsonArray())

            listOf(rootDoc.copy().apply { remove("ancestors") }) +
                    ancestors.map { it as JsonObject }
        }.distinctBy { it.getString("_id") }

        return buildGraphFromDocuments(allDocuments) { createMinimalEntity(it) }
    }
}