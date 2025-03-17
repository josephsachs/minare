package com.minare.persistence

import com.google.inject.Singleton
import com.minare.core.models.Entity
import com.minare.core.entity.EntityFactory
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Parent
import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.BulkOperation
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.await
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * MongoDB implementation of the EntityStore interface.
 * Handles entity storage, retrieval, and version management.
 */
@Singleton
class MongoEntityStore @Inject constructor(
    private val mongoClient: MongoClient,
    private val collection: String,
    private val entityFactory: EntityFactory,
    private val reflectionCache: ReflectionCache
) : EntityStore {

    private val log = LoggerFactory.getLogger(MongoEntityStore::class.java)

    /**
     * Builds a MongoDB aggregation pipeline to retrieve an entity and all its potential ancestors.
     *
     * @param entityId The ID of the entity to start with
     * @return A JsonObject containing the aggregation pipeline
     */
    suspend fun buildAncestorTraversalQuery(entityId: String): JsonObject {
        // Get all parent reference field paths from entity classes
        val parentRefPaths = getAllParentReferenceFieldPaths()

        // Build the aggregation pipeline
        val pipeline = JsonArray().apply {
            // Stage 1: Match the starting entity
            add(JsonObject().apply {
                put("${'$'}match", JsonObject().apply {
                    put("_id", entityId)
                })
            })

            // Stage 2: Use $graphLookup to find all potential ancestors
            add(JsonObject().apply {
                put("${'$'}graphLookup", JsonObject().apply {
                    put("from", "entities")
                    put("startWith", "${'$'}_id")
                    put("connectFromField", "_id")
                    put("connectToField", "state.*")
                    put("as", "ancestors")
                    put("maxDepth", 10)
                    put("depthField", "depth")
                })
            })
        }

        return JsonObject().put("pipeline", pipeline)
    }

    /**
     * Gets all parent reference field paths for all registered entity types.
     * This is useful for building MongoDB traversal queries.
     */
    private fun getAllParentReferenceFieldPaths(): List<String> {
        // Get classes from the entity factory
        val entityClasses = mutableListOf<Class<*>>()

        // Assuming entityFactory has a method to get registered class types
        // If not, we'll need another way to get the list of entity classes
        val entityTypeNames = entityFactory.getTypeNames()

        for (typeName in entityTypeNames) {
            entityFactory.useClass(typeName)?.let {
                entityClasses.add(it)
            }
        }

        return entityClasses.flatMap { entityClass ->
            reflectionCache.getFieldsWithAnnotation<Parent>(entityClass)
                .map { field ->
                    "state.${field.name}"
                }
        }.distinct()
    }

    /**
     * Executes the ancestor traversal query using coroutines.
     *
     * @param entityId The ID of the entity to start with
     * @return The aggregation results as a JsonArray
     */
    suspend fun getAncestorsRawAsync(entityId: String): JsonArray {
        val query = buildAncestorTraversalQuery(entityId)
        val results = JsonArray()

        // Create a promise we can await
        val promise = io.vertx.core.Promise.promise<JsonArray>()

        mongoClient.aggregate(collection, query.getJsonArray("pipeline"))
            .handler { item -> results.add(item) }
            .endHandler { promise.complete(results) }
            .exceptionHandler { promise.fail(it) }

        // Await the future completion
        return promise.future().await()
    }

    /**
     * {@inheritDoc}
     */
    override suspend fun getAncestorGraph(entityId: String): Graph<Entity, DefaultEdge> =
        transformResultsToEntityGraph(getAncestorsRawAsync(entityId))

    /**
     * Transforms MongoDB aggregation results into a graph of entity instances.
     *
     * @param results The MongoDB aggregation results
     * @return A directed graph where vertices are entity instances and edges represent parent-child relationships
     */
    fun transformResultsToEntityGraph(results: JsonArray): Graph<Entity, DefaultEdge> {
        if (results.isEmpty) {
            return DefaultDirectedGraph(DefaultEdge::class.java)
        }

        val graph = DefaultDirectedGraph<Entity, DefaultEdge>(DefaultEdge::class.java)

        // First pass: create all entity instances
        val entitiesById = results.map { it as JsonObject }
            .flatMap { json ->
                // Add the root entity
                val rootJson = JsonObject().apply {
                    put("_id", json.getString("_id"))
                    put("type", json.getString("type"))
                    put("version", json.getInteger("version", 1))
                }

                // Add all ancestors
                val ancestors = json.getJsonArray("ancestors", JsonArray())

                sequenceOf(rootJson) + ancestors.map { it as JsonObject }
            }
            .fold(mutableMapOf<String, Entity>()) { acc, json ->
                val id = json.getString("_id")
                if (!acc.containsKey(id)) {
                    val type = json.getString("type")
                    val entity = entityFactory.getNew(type).apply {
                        this._id = id
                        this.version = json.getInteger("version", 1)
                        this.type = type
                    }

                    graph.addVertex(entity)
                    acc[id] = entity
                }
                acc
            }

        // Second pass: connect entities
        // Process the root entity and all ancestors
        val allEntities = results.flatMap { rootJson ->
            rootJson as JsonObject
            val ancestors = rootJson.getJsonArray("ancestors", JsonArray())
            sequenceOf(rootJson) + ancestors.map { it as JsonObject }
        }.toList()

        val processedEntityIds = mutableSetOf<String>()
        allEntities.forEach { json ->
            val sourceId = json.getString("_id")

            // Skip if we've already processed this entity
            if (sourceId in processedEntityIds) {
                return@forEach
            }
            processedEntityIds.add(sourceId)

            val sourceType = json.getString("type")
            val sourceEntity = entitiesById[sourceId] ?: return@forEach

            // Get entity class for this type
            val entityClass = entityFactory.useClass(sourceType) ?: return@forEach

            // Get parent fields for this entity type using the reflection cache
            val parentFields = reflectionCache.getFieldsWithAnnotation<Parent>(entityClass)

            // For each parent reference field
            parentFields.forEach { field ->
                val fieldName = field.name

                // Get the value from the state object
                val state = json.getJsonObject("state", JsonObject())
                val fieldValue = state.getValue(fieldName)

                // Get the parent ID if this is a reference
                val parentId = extractParentId(fieldValue)

                if (parentId != null && entitiesById.containsKey(parentId)) {
                    val targetEntity = entitiesById[parentId]!!
                    graph.addEdge(sourceEntity, targetEntity)
                }
            }
        }

        return graph
    }

    /**
     * Extracts a parent entity ID from a field value.
     *
     * @param fieldValue The field value to extract from
     * @return The parent ID, or null if not found
     */
    private fun extractParentId(fieldValue: Any?): String? = when (fieldValue) {
        is String -> fieldValue
        is JsonObject -> fieldValue.getString("${'$'}id")
        else -> null
    }

    /**
     * {@inheritDoc}
     */
    override suspend fun updateVersions(entityIds: Set<String>): JsonObject {
        if (entityIds.isEmpty()) {
            return JsonObject()
        }

        val operations = entityIds.map { id ->
            val filter = JsonObject().put("_id", id)
            val update = JsonObject().put("${'$'}inc", JsonObject().put("version", 1))
            BulkOperation.createUpdate(filter, update)
        }

        val result = mongoClient.bulkWrite(collection, operations).await()

        return JsonObject().apply {
            put("updatedCount", result.modifiedCount)
            put("matchedCount", result.matchedCount)
        }
    }

    override suspend fun save(entity: Entity): Future<Entity> {
        val document = JsonObject()
            .put("_id", entity._id)
            .put("type", entity.type)
            .put("version", entity.version)
        val stateJson = JsonObject()

        document.put("state", stateJson)

        // Check if this is an update or new entity (based on id presence)
        return if (entity._id.isNullOrEmpty()) {
            // New entity - let MongoDB generate an ID and set initial version
            document.put("version", 1)

            mongoClient.insert(collection, document)
                .map { generatedId ->
                    // Update entity with the generated ID
                    entity._id = generatedId
                    entity
                }
                .onSuccess { log.debug("Created new entity with ID: {}", entity._id) }
        } else {
            // Existing entity - just update it, version handling will be done elsewhere
            val query = JsonObject()
                .put("_id", entity._id)

            val update = JsonObject().put("\$set", document)

            mongoClient.updateCollection(collection, query, update)
                .compose { result ->
                    if (result.docModified == 0L) {
                        // No document was updated - entity might not exist
                        Future.failedFuture("Entity not found: ${entity._id}")
                    } else {
                        Future.succeededFuture(entity)
                    }
                }
                .onSuccess { log.debug("Updated entity: {}", entity._id) }
        }
            .onFailure { err -> log.error("Failed to save entity: {}", entity._id, err) }
    }
}