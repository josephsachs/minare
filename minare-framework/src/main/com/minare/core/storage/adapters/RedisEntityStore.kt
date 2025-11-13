package com.minare.core.storage.adapters

import com.google.inject.Singleton
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Property
import com.minare.core.entity.annotations.State
import com.minare.core.entity.models.Entity
import com.minare.core.entity.services.EntityFieldDeserializer
import com.minare.core.entity.services.EntityPublishService
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.RedisAPI
import org.slf4j.LoggerFactory
import javax.inject.Inject
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.JsonSerializable
import io.vertx.core.json.JsonArray
import io.vertx.redis.client.Command
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge
import javax.naming.ServiceUnavailableException

@Singleton
class RedisEntityStore @Inject constructor(
    private val redisAPI: RedisAPI,
    private val reflectionCache: ReflectionCache,
    private val entityFactory: EntityFactory,
    private val publishService: EntityPublishService,
    private val entityStateDeserializer: EntityFieldDeserializer
) : StateStore {

    private val log = LoggerFactory.getLogger(RedisEntityStore::class.java)

    override suspend fun save(entity: Entity): Entity {
        val entityType = entity.type!!
        val stateJson = JsonObject()
        val propJson = JsonObject()

        entityFactory.useClass(entityType)?.let { entityClass ->
            val stateFields = reflectionCache.getFieldsWithAnnotation<State>(entityClass)
            for (field in stateFields) {
                field.isAccessible = true
                val value = field.get(entity)
                if (value != null) {
                    val jsonValue = serializeFieldValue(value)
                    stateJson.put(field.name, jsonValue)
                }
            }
        }

        entityFactory.useClass(entityType)?.let { entityClass ->
            val propFields = reflectionCache.getFieldsWithAnnotation<Property>(entityClass)
            for (field in propFields) {
                field.isAccessible = true
                val value = field.get(entity)
                if (value != null) {
                    val jsonValue = serializeFieldValue(value)
                    propJson.put(field.name, jsonValue)
                }
            }
        }

        val document = JsonObject()
            .put("_id", entity._id)
            .put("type", entityType)
            .put("version", entity.version ?: 1)
            .put("state", stateJson)
            .put("properties", propJson)

        redisAPI.jsonSet(listOf(entity._id!!, "$", document.encode())).await()
        redisAPI.sadd(listOf("entity:types:$entityType", entity._id!!)).await()

        return entity
    }

    /**
     * Serialize a field value for storage in Redis.
     * Handles entity references, collections, value objects, and primitives.
     */
    private fun serializeFieldValue(value: Any): Any {
        return when {
            // Entity reference - store just the ID
            value is Entity -> value._id

            // Enum - store the name as a string
            value is Enum<*> -> value.name

            // Collection - check if it contains entities
            value is Collection<*> -> {
                if (value.isEmpty()) {
                    JsonArray()
                } else if (value.all { it is Entity }) {
                    // Collection of entities - store IDs
                    JsonArray(value.filterIsInstance<Entity>().map { it._id })
                } else {
                    // Collection of primitives or data classes - store as-is
                    value
                }
            }

            // JsonObject - pass through
            value is JsonObject -> value
            value is JsonArray -> value

            // Primitives - pass through
            value is String || value is Number || value is Boolean -> value

            // JsonSerializable (legacy support during transition)
            value is JsonSerializable -> value.toJson()

            // Data classes and other serializable objects - use Jackson
            else -> {
                JsonObject(io.vertx.core.json.Json.encode(value))
            }
        }
    }

    /**
     * Update entity state
     */
    override suspend fun saveState(entityId: String, delta: JsonObject, incrementVersion: Boolean): JsonObject {
        val version = if (incrementVersion) {
            val response = redisAPI.send(
                Command.create("EVAL"),
                atomicIncrement(),
                "1",
                entityId,
                delta.encode()
            ).await()

            response.toLong()
        } else {
            val currentDocument = findEntityJson(entityId)
                ?: throw IllegalStateException("Entity not found: $entityId")

            val currentState = currentDocument.getJsonObject("state", JsonObject())

            delta.fieldNames().forEach { fieldName ->
                currentState.put(fieldName, delta.getValue(fieldName))
            }

            currentDocument.put("state", currentState)
            redisAPI.jsonSet(listOf(entityId, "$", currentDocument.encode())).await()
            currentDocument.getLong("version", 1L)
        }

        val updatedDocument = findEntityJson(entityId)!!

        publishService.publishStateChange(
            entityId,
            updatedDocument.getString("type"),
            version,
            delta
        )

        return updatedDocument
    }

    /**
     * Lua script for atomically incrementing version
     */
    private fun atomicIncrement(): String {
        return """
            local doc_json = redis.call('JSON.GET', KEYS[1])
            if not doc_json then
                error('Entity not found: ' .. KEYS[1])
            end
            
            local doc = cjson.decode(doc_json)
            local delta = cjson.decode(ARGV[1])
            
            -- Apply delta to state
            local state = doc.state or {}
            for key, value in pairs(delta) do
                state[key] = value
            end
            doc.state = state
            
            -- Increment version
            doc.version = (doc.version or 1) + 1
            
            redis.call('JSON.SET', KEYS[1], '${'$'}', cjson.encode(doc))
            return doc.version
        """
    }

    /**
     * Save properties to document. Properties are stored in a Json box similar to state,
     * and expect a delta. Updating properties does not result in a version increment.
     * Default is no pubsub notification.
     */
    override suspend fun saveProperties(entityId: String, delta: JsonObject, publish: Boolean): JsonObject {
        val currentDocument = findEntityJson(entityId)
            ?: throw IllegalStateException("Entity not found: $entityId")

        val currentProperties = currentDocument.getJsonObject("properties", JsonObject())

        delta.fieldNames().forEach { fieldName ->
            currentProperties.put(fieldName, delta.getValue(fieldName))
        }

        currentDocument.put("properties", currentProperties)
        redisAPI.jsonSet(listOf(entityId, "$", currentDocument.encode())).await()

        val updatedDocument = findEntityJson(entityId)!!

        if (publish) {
            publishService.publishStateChange(
                entityId,
                updatedDocument.getString("type"),
                currentDocument.getLong("version", 1L),
                delta
            )
        }

        return updatedDocument
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
                // TEMPORARY DEBUG
                val document = JsonArray(item.toString()).getJsonObject(0)

                val entityId = document.getString("_id")
                log.warn("StateStore found Entity document with invalid state field for ${entityId}: ${e.message}", e)
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
                    val value = entityStateDeserializer.deserialize(
                        state.getValue(field.name),
                        field
                    )
                    if (value != null) {
                        field.set(entity, value)
                    }

                } catch (e: Exception) {
                    // TODO: Fix serialization so this bug stops happening
                    log.warn("StateStore found Entity document with invalid property field for ${entity._id}: ${e.message}", e)

                }
            }
        }

        return entity
    }

    /**
     * Populate state fields using reflection
     */
    override suspend fun setEntityProperties(entity: Entity, entityType: String, properties: JsonObject): Entity {
        entityFactory.useClass(entityType)?.let { entityClass ->
            val propertyFields = reflectionCache.getFieldsWithAnnotation<Property>(entityClass)

            for (field in propertyFields) {
                field.isAccessible = true
                try {
                    val value = entityStateDeserializer.deserialize(
                        properties.getValue(field.name),
                        field
                    )
                    if (value != null) {
                        field.set(entity, value)
                    }

                } catch (e: Exception) {
                    // TODO: Fix serialization so this bug stops happening
                    log.warn("StateStore found Entity document with invalid property field for ${entity._id}")

                }
            }
        }

        return entity
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
                vertex.put("properties", fullEntity.getJsonObject("properties", JsonObject()))
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