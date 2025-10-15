package com.minare.core.storage.adapters

import com.google.inject.Singleton
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.State
import com.minare.core.entity.models.Entity
import com.minare.core.entity.services.EntityPublishService
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.RedisAPI
import org.slf4j.LoggerFactory
import javax.inject.Inject
import com.minare.core.storage.interfaces.StateStore
import io.vertx.core.json.JsonArray
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge
import javax.naming.ServiceUnavailableException
import kotlin.reflect.typeOf

@Singleton
class RedisEntityStore @Inject constructor(
    private val redisAPI: RedisAPI,
    private val reflectionCache: ReflectionCache,
    private val entityFactory: EntityFactory,
    private val publishService: EntityPublishService
) : StateStore {

    private val log = LoggerFactory.getLogger(RedisEntityStore::class.java)

    override suspend fun save(entity: Entity): Entity {
        val entityType = entity.type!!
        val stateJson = JsonObject()

        entityFactory.useClass(entityType)?.let { entityClass ->
            val stateFields = reflectionCache.getFieldsWithAnnotation<State>(entityClass)
            for (field in stateFields) {
                field.isAccessible = true
                val value = field.get(entity)
                if (value != null) {
                    stateJson.put(field.name, value)
                }
            }
        }

        val document = JsonObject()
            .put("_id", entity._id)
            .put("type", entityType)
            .put("version", entity.version ?: 1)
            .put("state", stateJson)

        redisAPI.jsonSet(listOf(entity._id!!, "$", document.encode())).await()
        redisAPI.sadd(listOf("entity:types:$entityType", entity._id!!)).await()

        return entity
    }

    /**
     * Update entity state
     */
    override suspend fun mutateState(entityId: String, delta: JsonObject, incrementVersion: Boolean): JsonObject {
        // Get the current entity using our findEntityJson method
        val currentDocument = findEntityJson(entityId)
            ?: throw IllegalStateException("Entity not found: $entityId")

        val currentState = currentDocument.getJsonObject("state", JsonObject())

        // Apply delta to state
        delta.fieldNames().forEach { fieldName ->
            currentState.put(fieldName, delta.getValue(fieldName))
        }

        var version = currentDocument.getLong("version", 1L)
        if (incrementVersion) {
            version++
            currentDocument.put("version", version)
        }

        currentDocument.put("state", currentState)

        // Store updated document
        redisAPI.jsonSet(listOf(entityId, "$", currentDocument.encode())).await()

        // Publish the change
        publishService.publishStateChange(
            entityId,
            currentDocument.getString("type"),
            version,
            delta
        )

        return currentDocument
    }

    /**
     * Take JsonObject from Redis Entity store and return Entity (not including state)
     */
    suspend fun getEntity(document: JsonObject): Entity? {
        val entityId = document.getString("_id")
        val entityType = document.getString("type")
        val version = document.getLong("version", 1L)

        if (null in listOf(entityId, entityType, version)) {
            log.warn("Found missing or malformed entity for $entityId")
            return null
        }

        val entity = entityFactory.getNew(entityType).apply {
            this._id = entityId
            this.version = version
            this.type = entityType
        }

        return entity
    }

    /**
     * Find a single Entity
     */
    override suspend fun findOne(entityId: String): Entity? {
        val response = redisAPI.jsonGet(listOf(entityId, ".")).await()
        val document = JsonObject(response.toString())

        return getEntity(document)
    }

    /**
     * Find a list of entities
     */
    override suspend fun find(entityIds: List<String>): Map<String, Entity> {
        if (entityIds.isEmpty()) {
            return emptyMap()
        }

        val result = mutableMapOf<String, Entity>()
        val response = redisAPI.jsonMget(entityIds + "$").await()

        val collection = JsonArray(response.toString())

        for (item in collection) {
            try {
                val document = JsonArray(item.toString()).getJsonObject(0)

                val entityId = document.getString("_id")
                val entity = getEntity(document)

                if (entity != null) {
                    result[entityId] = entity
                }
            } catch (e: Exception) {
                log.warn("Error fetching entity from $entityIds: ${e.message}")
            }
        }

        return result
    }

    /**
     * Populate state fields using reflection
     */
    override suspend fun setEntityState(entity: Entity, entityType: String, state: JsonObject): Entity {
        entityFactory.useClass(entityType)?.let { entityClass ->
            val stateFields = reflectionCache.getFieldsWithAnnotation<State>(entityClass)

            for (field in stateFields) {
                field.isAccessible = true
                try {
                    val value = state.getValue(field.name)
                    if (value != null) {
                        field.set(entity, convertValue(value))
                    }

                } catch (e: Exception) {
                    // TODO: Fix serialization so this bug stops happening
                    //log.warn("StateStore found Entity document with invalid state field for ${entity._id}")

                }
            }
        }

        return entity
    }

    private fun convertValue(value: Any): Any {
        val convertedValue = when (value) {
            is JsonArray -> {
                value.list.mapNotNull { it?.toString() }.toMutableList()
            }
            is JsonObject -> {
                value.map
            }
            else -> value
        }
        return value
    }

    /**
     * Hydrates graph nodes with full entity state from Redis
     * Modifies the graph in-place by merging Redis data into each node
     * @param graph Graph with nodes containing minimal entity info (id, type, version)
     * @return The same graph structure but with nodes containing full state
     */
    override suspend fun hydrateGraph(graph: Graph<JsonObject, DefaultEdge>) {
        val entityIds = graph.vertexSet().mapNotNull { it.getString("_id") }

        if (entityIds.isEmpty()) {
            return
        }

        // Batch fetch all entities from Redis
        val fullEntities = findEntitiesJsonByIds(entityIds)

        // Merge full state into each graph node
        for (vertex in graph.vertexSet()) {
            val entityId = vertex.getString("_id") ?: continue
            fullEntities[entityId]?.let { fullEntity ->
                // Merge the Redis state into the MongoDB node
                // Keep the graph structure but add the full state
                vertex.put("state", fullEntity.getJsonObject("state", JsonObject()))
                // Add any other fields you need from Redis
            }
        }
    }

    /**
     * Finds multiple entities by their IDs and returns them as JsonObjects
     * @param entityIds List of entity IDs to fetch
     * @return Map of entity IDs to JsonObjects
     */
    override suspend fun findEntitiesJsonByIds(entityIds: List<String>): Map<String, JsonObject> {
        if (entityIds.isEmpty()) {
            return emptyMap()
        }

        val result = mutableMapOf<String, JsonObject>()
        val response = redisAPI.jsonMget(entityIds + "$").await()

        val collection = JsonArray(response.toString())

        for (item in collection) {
            try {
                val document = JsonArray(item.toString()).getJsonObject(0)
                val entityId = document.getString("_id")

                if (entityId != null) {
                    result[entityId] = document
                }
            } catch (e: Exception) {
                log.warn("Error fetching entity JSON from $entityIds: ${e.message}")
            }
        }

        return result
    }

    /**
     * Finds an entity by ID and returns it as an Entity
     * @param entityId The ID of the entity to fetch
     * @return The entity as a JsonObject, or null if not found
     *
     * @deprecated Use find()
     */
    override suspend fun findEntity(entityId: String): Entity? {
        return find(listOf(entityId))[entityId]
    }

    /**
     * Get the type of an entity
     * @param entityId
     */
    override suspend fun findEntityType(entityId: String): String? {
        if (entityId.isEmpty()) {
            return null
        }

        try {
            // If stored as hash with just entityId as key
            val response = redisAPI.jsonGet(listOf(entityId, "$.type")).await()

            val jsonArray = JsonArray(response.toString())
            val responseValue = jsonArray.getString(0)

            return responseValue
        } catch (e: Exception) {
            // Todo: Send infrastructure alert
            throw ServiceUnavailableException("Could not fetch from Redis: ${e.message}")
        }
    }

    /**
     * Finds all entity keys for a given entity type
     * @param type String
     * @return List<String>
     */
    override suspend fun findKeysByType(type: String): List<String> {
        val response = redisAPI.smembers("entity:types:$type").await()
        val uuids = response?.map { it.toString() } ?: emptyList()
        return uuids
    }

    /**
     * Finds multiple entities by their IDs and returns as JsonObjects
     * @param entityIds List of entity IDs to fetch
     * @return Map of entity IDs to JsonObject documents
     */
    override suspend fun findEntitiesJson(entityIds: List<String>): Map<String, JsonObject> {
        if (entityIds.isEmpty()) {
            return emptyMap()
        }

        val result = mutableMapOf<String, JsonObject>()
        val response = redisAPI.jsonMget(entityIds + listOf("$")).await()

        if (response == null) {
            log.warn("jsonMget returned null for keys: $entityIds")
            return emptyMap()
        }

        val collection = JsonArray(response.toString())

        for ((index, item) in collection.withIndex()) {
            try {
                if (item != null) {
                    // Each item is wrapped in an array - extract element 0
                    val document = JsonArray(item.toString()).getJsonObject(0)
                    val entityId = entityIds[index]
                    result[entityId] = document
                }
            } catch (e: Exception) {
                log.warn("Error parsing entity JSON at index $index: ${e.message}")
            }
        }

        return result
    }

    /**
     * Finds an entity by ID and returns it as a JsonObject
     * @param entityId The ID of the entity to fetch
     * @return The entity as a JsonObject, or null if not found
     */
    override suspend fun findEntityJson(entityId: String): JsonObject? {
        return findEntitiesJsonByIds(listOf(entityId))[entityId]
    }

    /**
     * Get all entity keys from the store
     * @return List of all entity IDs (excluding frame deltas and other non-entity keys)
     */
    override suspend fun getAllEntityKeys(): List<String> {
        val entityKeys = mutableListOf<String>()
        var cursor = "0"

        try {
            do {
                val scanResult = redisAPI.scan(listOf(cursor, "COUNT", "100")).await()
                cursor = scanResult.get(0).toString()
                val keys = scanResult.get(1)

                if (keys != null) {
                    keys.forEach { key ->
                        val keyStr = key.toString()
                        // Exclude frame delta keys
                        if (!keyStr.startsWith("frame:")) {
                            entityKeys.add(keyStr)
                        }
                    }
                }
            } while (cursor != "0")

            // Sort for consistent ordering across calls
            entityKeys.sort()

            return entityKeys
        } catch (e: Exception) {
            log.error("Error getting entity keys", e)
            return emptyList()
        }
    }
}