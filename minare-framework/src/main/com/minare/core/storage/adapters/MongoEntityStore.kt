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
import io.vertx.kotlin.coroutines.await
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge
import org.slf4j.LoggerFactory
import javax.inject.Inject
import com.minare.core.storage.interfaces.EntityQueryStore
import com.minare.core.storage.interfaces.EntityStore
import com.minare.core.storage.interfaces.WriteBehindStore

/**
 * MongoDB implementation of the EntityStore interface.
 * Handles entity storage, retrieval, and version management.
 */
@Singleton
class MongoEntityStore @Inject constructor(
    private val mongoClient: MongoClient,
    private val entityFactory: EntityFactory,
    private val reflectionCache: ReflectionCache
) : EntityStore, EntityQueryStore, WriteBehindStore {

    private val log = LoggerFactory.getLogger(MongoEntityStore::class.java)

    // Default collection name for all entities
    private val collection = "entities"

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
                    put("from", collection)
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

        val promise = io.vertx.core.Promise.promise<JsonArray>()

        mongoClient.aggregate(collection, query.getJsonArray("pipeline"))
            .handler { item -> results.add(item) }
            .endHandler { promise.complete(results) }
            .exceptionHandler { promise.fail(it) }

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
                        this.version = json.getLong("version", 1)
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

    /**
     * Inserts or updates a given entity
     * @param entity A new Entity object with type and version specified
     * @return The updated entity document with id
     */
    override suspend fun save(entity: Entity): Entity {
        log.debug("Saving entity of type: ${entity.type} to collection: $collection")

        val document = JsonObject()
            .put("type", entity.type)
            .put("version", entity.version)
        val stateJson = JsonObject()

        // Extract state from the entity
        val entityType = entity.type
        if (entityType != null) {
            entityFactory.useClass(entityType)?.let { entityClass ->
                // Get fields with the @State annotation
                val stateFields = reflectionCache.getFieldsWithAnnotation<com.minare.core.entity.annotations.State>(entityClass)
                for (field in stateFields) {
                    field.isAccessible = true
                    try {
                        val value = field.get(entity)
                        if (value != null) {
                            stateJson.put(field.name, value)
                        }
                    } catch (e: Exception) {
                        log.warn("Error getting field ${field.name} from entity", e)
                    }
                }
            }
        } else {
            log.warn("Cannot extract state: entity type is null")
        }

        document.put("state", stateJson)

        try {
            if (entity._id.isNullOrEmpty()) {
                // New entity - let MongoDB generate an ID and set initial version
                document.put("version", 1)

                val generatedId = mongoClient.insert(collection, document).await()
                // Update entity with the generated ID
                entity._id = generatedId
                log.debug("Created new entity with ID: {}", entity._id)
            } else {
                val query = JsonObject().put("_id", entity._id)
                val update = JsonObject().put("\$set", document)

                val result = mongoClient.updateCollection(collection, query, update).await()
                if (result.docModified == 0L) {
                    // No document was updated - entity might not exist
                    throw IllegalStateException("Entity not found: ${entity._id}")
                }
                log.debug("Updated entity: {}", entity._id)
            }

            return entity
        } catch (err: Exception) {
            log.error("Failed to save entity: {}", entity._id, err)
            throw err
        }
    }

    /**
     * Updates an entity's state fields in MongoDB based on the provided delta
     * @param entityId The ID of the entity to update
     * @param delta The filtered delta containing only fields that passed consistency checks
     * @return The updated entity document
     */
    override suspend fun mutateState(entityId: String, delta: JsonObject): JsonObject {
        if (delta.isEmpty()) {
            return JsonObject()
        }

        val setOp = JsonObject()

        delta.fieldNames().forEach { fieldName ->
            setOp.put("state.$fieldName", delta.getValue(fieldName))
        }

        val update = JsonObject()
            .put("\$set", setOp)
            .put("\$inc", JsonObject().put("version", 1))

        val query = JsonObject()
            .put("_id", entityId)

        try {
            val updateResult = mongoClient.updateCollection(collection, query, update).await()

            if (updateResult.docModified == 0L) {
                // No document was updated - entity might not exist
                throw IllegalStateException("Entity not found: $entityId")
            }

            val result = mongoClient.findOne(collection, JsonObject().put("_id", entityId), JsonObject()).await()

            log.debug("Updated entity state: $entityId")
            return result
        } catch (err: Exception) {
            log.error("Failed to update entity state: $entityId", err)
            throw err
        }
    }

    /**
     * Fetches multiple entities by their IDs
     *
     * @param entityIds List of entity IDs to fetch
     * @return Map of entity IDs to entity objects
     */
    override suspend fun findEntitiesByIds(entityIds: List<String>): Map<String, Entity> {
        if (entityIds.isEmpty()) {
            return emptyMap()
        }

        try {
            log.debug("Finding ${entityIds.size} entities by IDs")

            val orCriteria = JsonArray()
            entityIds.forEach { id ->
                orCriteria.add(JsonObject().put("_id", id))
            }
            val query = JsonObject().put("\$or", orCriteria)

            log.debug("MongoDB query: ${query.encode()}")

            var results = mongoClient.find(collection, query).await()

            log.debug("Query results size: ${results.size}")

            if (results.isEmpty()) {
                log.debug("No results with \$or query, trying with individual queries")

                val individualResults = mutableListOf<JsonObject>()
                for (id in entityIds) {
                    val singleQuery = JsonObject().put("_id", id)
                    log.debug("Individual query: ${singleQuery.encode()}")

                    val singleResult = mongoClient.findOne(collection, singleQuery, JsonObject()).await()
                    if (singleResult != null) {
                        individualResults.add(singleResult)
                    }
                }

                if (individualResults.isNotEmpty()) {
                    log.debug("Found ${individualResults.size} entities using individual queries")
                    results = individualResults
                }
            }

            // Convert the document results to Entity objects
            return results.mapNotNull { document ->
                try {
                    val id = document.getString("_id")
                    val type = document.getString("type")
                    val version = document.getLong("version", 1L)
                    val state = document.getJsonObject("state", JsonObject())

                    // Create a new entity instance of the appropriate type
                    val entityType = type ?: "unknown"
                    val entity = entityFactory.getNew(entityType).apply {
                        this._id = id
                        this.version = version
                        this.type = entityType

                    }

                    id to entity
                } catch (e: Exception) {
                    log.error("Error deserializing entity: ${document.getString("_id")}", e)
                    null
                }
            }.toMap()
        } catch (err: Exception) {
            log.error("Failed to fetch entities by IDs", err)
            throw err
        }
    }

    /**
     * Builds a graph of entities based on a list of entity IDs
     *
     * @param entityIds List of entity IDs to include in the graph
     * @return A graph containing the requested entities and their relationships
     */
    override suspend fun buildEntityGraph(entityIds: List<String>): Graph<Entity, DefaultEdge> {
        if (entityIds.isEmpty()) {
            return DefaultDirectedGraph(DefaultEdge::class.java)
        }

        // First, fetch all the requested entities as JSON documents
        // Create an $or query to handle potential ObjectId vs String ID issues
        val orCriteria = JsonArray()
        entityIds.forEach { id ->
            orCriteria.add(JsonObject().put("_id", id))
        }
        val query = JsonObject().put("\$or", orCriteria)

        // Log the query for debugging
        log.debug("Building entity graph with query: ${query.encode()}")

        val documents = mongoClient.find(collection, query).await()

        log.debug("Found ${documents.size} documents for graph")

        if (documents.isEmpty()) {
            return DefaultDirectedGraph(DefaultEdge::class.java)
        }

        // Create a new directed graph
        val graph = DefaultDirectedGraph<Entity, DefaultEdge>(DefaultEdge::class.java)
        val entitiesById = mutableMapOf<String, Entity>()

        // Add all entities as vertices
        for (document in documents) {
            val id = document.getString("_id")
            val type = document.getString("type") ?: "unknown"
            val version = document.getLong("version", 1L)

            // Create a minimal entity with just the core properties
            val entity = entityFactory.getNew(type).apply {
                this._id = id
                this.version = version
                this.type = type
                // We don't need to fully deserialize the state
            }

            graph.addVertex(entity)
            entitiesById[id] = entity
        }

        // Now analyze each document to find relationships
        for (document in documents) {
            val sourceId = document.getString("_id")
            val state = document.getJsonObject("state", JsonObject())
            val sourceEntity = entitiesById[sourceId] ?: continue

            // Look for parent references in the state
            val parentId = state.getString("parentId")
            if (parentId != null && entitiesById.containsKey(parentId)) {
                val parentEntity = entitiesById[parentId]!!
                graph.addEdge(sourceEntity, parentEntity)
            }
        }

        return graph
    }

    /**
     * Builds a graph of Mongo documents based on a list of entity IDs
     *
     * @param entityIds List of entity IDs to include in the graph
     * @return A graph containing the requested entity documents and their relationships
     */
    override suspend fun buildDocumentGraph(entityIds: List<String>): Graph<JsonObject, DefaultEdge> {
        if (entityIds.isEmpty()) {
            return DefaultDirectedGraph(DefaultEdge::class.java)
        }

        // Fetch all documents using $or query for better compatibility with ObjectId
        val orCriteria = JsonArray()
        entityIds.forEach { id ->
            orCriteria.add(JsonObject().put("_id", id))
        }
        val query = JsonObject().put("\$or", orCriteria)

        // Log the query for debugging
        log.debug("Building document graph with query: ${query.encode()}")

        val documents = mongoClient.find(collection, query).await()

        log.debug("Found ${documents.size} documents for graph")

        if (documents.isEmpty()) {
            // Try individual queries as fallback
            log.debug("No results with \$or query, trying with individual queries")

            val individualResults = mutableListOf<JsonObject>()
            for (id in entityIds) {
                val singleQuery = JsonObject().put("_id", id)
                log.debug("Individual query: ${singleQuery.encode()}")

                val singleResult = mongoClient.findOne(collection, singleQuery, JsonObject()).await()
                if (singleResult != null) {
                    individualResults.add(singleResult)
                    log.debug("Found document with ID: $id")
                } else {
                    log.debug("No document found with ID: $id")
                }
            }

            if (individualResults.isEmpty()) {
                return DefaultDirectedGraph(DefaultEdge::class.java)
            }

            return buildDocumentGraphFromDocuments(individualResults)
        }

        return buildDocumentGraphFromDocuments(documents)
    }

    /**
     * Helper method to build a document graph from a list of MongoDB documents
     */
    private fun buildDocumentGraphFromDocuments(documents: List<JsonObject>): Graph<JsonObject, DefaultEdge> {
        // Create document lookup map for efficiency
        val documentsById = documents.associateBy { it.getString("_id") }

        // Create a new directed graph
        val graph = DefaultDirectedGraph<JsonObject, DefaultEdge>(DefaultEdge::class.java)

        // Add all documents as vertices
        for (document in documents) {
            graph.addVertex(document)
        }

        // Now analyze each document to find relationships
        for (document in documents) {
            val state = document.getJsonObject("state", JsonObject())
            val parentId = state.getString("parentId")

            if (parentId != null && documentsById.containsKey(parentId)) {
                val parentDocument = documentsById[parentId]!!
                graph.addEdge(document, parentDocument)
            }
        }

        return graph
    }

    /**
     * Traverses from a document to its parents in a JsonObject graph, collecting parent documents.
     * This replaces Entity.traverseParents() with JsonObject-based logic.
     */
    override fun traverseParents(
        graph: Graph<JsonObject, DefaultEdge>,
        document: JsonObject,
        visited: MutableSet<String>
    ): List<JsonObject> {
        document.getString("_id")?.let { visited.add(it) }
        val parents = mutableListOf<JsonObject>()

        val outEdges = graph.outgoingEdgesOf(document)
        outEdges.forEach { edge ->
            val parent = graph.getEdgeTarget(edge)
            val parentId = parent.getString("_id")

            if (parentId != null && parentId !in visited) {
                parents.add(parent)
                parents.addAll(traverseParents(graph, parent, visited))
            }
        }

        return parents
    }

    /**
     * Persist entity document for write-behind storage (WriteBehindStore interface)
     * Updated to use JsonObject instead of Entity for consistency.
     */
    override suspend fun persistForWriteBehind(entityDocument: JsonObject): JsonObject {
        log.debug("Write-behind persist for entity: {}", entityDocument.getString("_id"))

        val entityId = entityDocument.getString("_id")
            ?: throw IllegalArgumentException("Entity document must have _id field")

        // Use MongoDB save logic for write-behind persistence
        val query = JsonObject().put("_id", entityId)
        val update = JsonObject().put("${'$'}set", entityDocument)

        // Upsert the document to MongoDB
        val result = mongoClient.findOneAndReplace(collection, query, entityDocument).await()

        if (result == null) {
            // Document didn't exist, insert it
            mongoClient.insert(collection, entityDocument).await()
            log.debug("Write-behind: Inserted new document for entity {}", entityId)
        } else {
            log.debug("Write-behind: Updated existing document for entity {}", entityId)
        }

        return entityDocument
    }
}