package com.minare.core.storage.adapters

import com.google.inject.Singleton
import com.minare.core.entity.services.EntityInspector
import com.minare.core.entity.models.Entity
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.annotations.Child
import com.minare.core.entity.annotations.Parent
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.BulkOperation
import io.vertx.ext.mongo.MongoClient
import io.vertx.ext.mongo.WriteOption
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory
import javax.inject.Inject
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
    private val entityInspector: EntityInspector
) : EntityGraphStore {

    companion object {
        const val COLLECTION_NAME = "entity_graph"
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
            if (entity._id.startsWith("unsaved-")) {
                // New entity - insert and let MongoDB generate an ID
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

        val fieldNames = entityInspector
            .getFieldsOfType(entityId, listOf(Parent::class, Child::class))
            .map { it.name }.toSet()

        val filteredDelta = delta.fieldNames()
            .filter { it in fieldNames }
            .fold(JsonObject()) { acc, field ->
                acc.put("state.$field", delta.getValue(field))
            }

        if (filteredDelta.isEmpty) {
            log.debug("No relationship fields to update for entity: $entityId")
            return JsonObject()
        }

        val update = JsonObject()
            .put("\$set", filteredDelta)

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

    private suspend fun buildEntityDocument(entity: Entity): JsonObject {
        val document = JsonObject()
            .put("type", entity.type)

        // Extract only relationship fields
        val relationshipFields = entityInspector.getFieldsOfType(entity, listOf(Child::class, Parent::class))

        // Build state object with field names and values
        val relationshipState = JsonObject()
        relationshipFields.forEach { field ->
            field.isAccessible = true
            val fieldValue = field.get(entity)
            if (fieldValue != null) {
                relationshipState.put(field.name, fieldValue)
            }
        }

        // Always include state field, even if empty
        document.put("state", relationshipState)

        return document
    }

    fun createMinimalEntity(document: JsonObject): Entity {
        val type = document.getString("type") ?: "unknown"
        return entityFactory.getNew(type).apply {
            _id = document.getString("_id")
            version = document.getLong("version", 1L)
            this.type = type
        }
    }

    suspend fun fetchDocumentsByIds(entityIds: List<String>): List<JsonObject> {
        val query = JsonObject().put(
            "\$or",
            JsonArray(entityIds.map { JsonObject().put("_id", it) })
        )

        return mongoClient.find(COLLECTION_NAME, query).await()
    }

    suspend fun executeAggregation(pipeline: JsonArray): JsonArray {
        val results = JsonArray()
        val promise = io.vertx.core.Promise.promise<JsonArray>()

        mongoClient.aggregate(COLLECTION_NAME, pipeline)
            .handler { results.add(it) }
            .endHandler { promise.complete(results) }
            .exceptionHandler { promise.fail(it) }

        return promise.future().await()
    }

    fun extractEntityId(fieldValue: Any?): String? = when (fieldValue) {
        is String -> fieldValue
        is JsonObject -> fieldValue.getString("\$id") ?: fieldValue.getString("_id")
        else -> null
    }
}