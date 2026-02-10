package com.minare.core.storage.adapters

import com.google.inject.Singleton
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Property
import com.minare.core.entity.annotations.State
import com.minare.core.entity.models.Entity
import com.minare.core.entity.services.EntityFieldDeserializer
import com.minare.core.entity.services.EntityFieldSerializer
import com.minare.core.entity.services.EntityPublishService
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.RedisAPI
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import com.minare.core.storage.interfaces.StateStore
import com.minare.exceptions.EntityStorageException
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
    private val deserializer: EntityFieldDeserializer,
    private val serializer: EntityFieldSerializer,
) : StateStore {
    private val log = LoggerFactory.getLogger(RedisEntityStore::class.java)

    override suspend fun save(entity: Entity): Entity {
        val entityType = entity.type
        val stateJson = JsonObject()
        val propJson = JsonObject()

        entityFactory.useClass(entityType)?.let { entityClass ->
            val stateFields = reflectionCache.getFieldsWithAnnotation<State>(entityClass)
            val propFields = reflectionCache.getFieldsWithAnnotation<Property>(entityClass)

            for (field in stateFields) {
                field.isAccessible = true
                val value = field.get(entity)
                if (value != null) {
                    val jsonValue = serializer.serialize(value)
                    stateJson.put(field.name, jsonValue)
                }
            }

            for (field in propFields) {
                field.isAccessible = true
                val value = field.get(entity)
                if (value != null) {
                    val jsonValue = serializer.serialize(value)
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

        redisAPI.jsonSet(listOf(entity._id, "$", document.encode())).await()
        redisAPI.sadd(listOf("entity:types:$entityType", entity._id)).await()

        return entity
    }

    /**
     * Update entity state
     */
    override suspend fun saveState(entityId: String, delta: JsonObject, incrementVersion: Boolean): JsonObject {
        val version = if (incrementVersion) {
            val serializedDelta = serializeDelta(delta)

            val response = redisAPI.send(
                Command.create("EVAL"),
                versionIncrementScript(),
                "1",
                entityId,
                serializedDelta.encode()
            ).await()

            response.toLong()
        } else {
            val currentDocument = findOneJson(entityId)
                ?: throw EntityStorageException("Entity not found: $entityId")

            val currentState = currentDocument.getJsonObject("state", JsonObject())

            val serializedDelta = serializeDelta(delta)
            serializedDelta.fieldNames().forEach { fieldName ->
                currentState.put(fieldName, serializedDelta.getValue(fieldName))
            }

            currentDocument.put("state", currentState)
            redisAPI.jsonSet(listOf(entityId, "$", currentDocument.encode())).await()
            currentDocument.getLong("version", 1L)
        }

        val updatedDocument = findOneJson(entityId)
            ?: throw EntityStorageException("Updated document not found for $entityId after set command")

        publishService.publishStateChange(
            entityId,
            updatedDocument.getString("type"),
            version,
            delta
        )

        return updatedDocument
    }

    /**
     * Batch updates state for multiple entities, incrementing versions.
     * Uses a Lua script for single round-trip execution.
     */
    override suspend fun batchSaveState(updates: Map<String, JsonObject>) {
        if (updates.isEmpty()) return

        val entityIds = updates.keys.toList()
        val deltas = entityIds.map { serializeDelta(updates[it]!!).encode() }

        val args = mutableListOf<String>()
        args.add(batchMergeDeltasScript())
        args.add(entityIds.size.toString())   // numkeys
        args.addAll(entityIds)                // KEYS
        args.addAll(deltas)                   // ARGV (but ARGV[1] will be first delta, not numkeys)

        redisAPI.send(Command.create("EVAL"), *args.toTypedArray()).await()

        // Publish state changes
        for ((entityId, delta) in updates) {
            val updatedDoc = findOneJson(entityId)
            if (updatedDoc != null) {
                publishService.publishStateChange(
                    entityId,
                    updatedDoc.getString("type"),
                    updatedDoc.getLong("version") ?: 1L,
                    delta
                )
            }
        }

        log.debug("Batch updated state for {} entities", updates.size)
    }

    /**
     * Save properties to document. Properties are stored in a Json box similar to state,
     * and expect a delta. Updating properties does not result in a version increment.
     * Default is no pubsub notification.
     */
    override suspend fun saveProperties(entityId: String, delta: JsonObject, publish: Boolean): JsonObject {
        val currentDocument = findOneJson(entityId)
            ?: throw IllegalStateException("Entity not found: $entityId")

        val currentProperties = currentDocument.getJsonObject("properties", JsonObject())

        val serializedDelta = serializeDelta(delta)
        serializedDelta.fieldNames().forEach { fieldName ->
            currentProperties.put(fieldName, serializedDelta.getValue(fieldName))
        }

        currentDocument.put("properties", currentProperties)
        redisAPI.jsonSet(listOf(entityId, "$", currentDocument.encode())).await()

        val updatedDocument = findOneJson(entityId)

        return updatedDocument
            ?: throw EntityStorageException("Failed to save properties for nonexistent entity $entityId")
    }

    /**
     * Deletes an entity from the state store
     * @param entityId The ID of the entity to delete
     * @return true if entity was deleted, false if not found
     */
    override suspend fun delete(entityId: String): Boolean {
        // Get entity type before deletion (needed to remove from type set)
        // Use a try-catch since findEntityType throws on missing entities
        val entityType = try {
            findEntityType(entityId)
        } catch (e: Exception) {
            log.warn("Cannot delete entity {}: not found or error fetching type", entityId)
            return false
        }

        if (entityType == null) {
            log.warn("Cannot delete entity {}: no type found", entityId)
            return false
        }

        // Delete the entity document
        val deleted = redisAPI.del(listOf(entityId)).await()

        // Remove from type set
        redisAPI.srem(listOf("entity:types:$entityType", entityId)).await()

        log.debug("Deleted entity {} of type {}", entityId, entityType)
        return deleted.toInteger() > 0
    }

    private fun serializeDelta(delta: JsonObject): JsonObject {
        val serialized = JsonObject()
        delta.fieldNames().forEach { fieldName ->
            val rawValue = delta.getValue(fieldName)
            if (rawValue != null) {
                serialized.put(fieldName, serializer.serialize(rawValue))
            } else {
                serialized.putNull(fieldName)
            }
        }
        return serialized
    }

    /**
     * Take JsonObject from Redis Entity store and return Entity (not including state)
     */
    private suspend fun getEntity(document: JsonObject): Entity? {
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
                log.warn("StateStore encountered an issue fetching this entity: ${e.message}", e)
            }
        }

        return result
    }

    /**
     * Populate all @State fields using reflection
     * Used in hydration
     * @param state incoming
     */
    override suspend fun setEntityState(entity: Entity, entityType: String, state: JsonObject): Entity {
        entityFactory.useClass(entityType)?.let { entityClass ->
            val stateFields = reflectionCache.getFieldsWithAnnotation<State>(entityClass)

            for (field in stateFields) {
                field.isAccessible = true
                try {
                    val value = deserializer.deserialize(
                        state.getValue(field.name),
                        field
                    )
                    if (value != null) {
                        field.set(entity, value)
                    }

                } catch (e: Exception) {
                    log.warn("StateStore found Entity document with invalid state field for ${entity._id}: ${field.name}, ${field.type};\\n ${e.message}", e)
                }
            }
        }

        return entity
    }

    /**
     * Hydrates @Property fields with data from
     * @param properties
     */
    override suspend fun setEntityProperties(entity: Entity, entityType: String, properties: JsonObject): Entity {
        entityFactory.useClass(entityType)?.let { entityClass ->
            val propertyFields = reflectionCache.getFieldsWithAnnotation<Property>(entityClass)

            for (field in propertyFields) {
                field.isAccessible = true
                try {
                    val value = deserializer.deserialize(
                        properties.getValue(field.name),
                        field
                    )
                    if (value != null) {
                        field.set(entity, value)
                    }

                } catch (e: Exception) {
                    log.warn("StateStore found Entity document with invalid property field for ${entity._id}: ${e.message}", e)
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
        val fullEntities = findJsonByIds(entityIds)

        for (vertex in graph.vertexSet()) {
            val entityId = vertex.getString("_id") ?: continue
            fullEntities[entityId]?.let { fullEntity ->
                vertex.put("state", fullEntity.getJsonObject("state", JsonObject()))
                vertex.put("properties", fullEntity.getJsonObject("properties", JsonObject()))
            }
        }
    }

    /**
     * Finds multiple entities by their IDs and returns them as JsonObjects
     * @param entityIds List of entity IDs to fetch
     * @return Map of entity IDs to JsonObjects
     */
    override suspend fun findJsonByIds(entityIds: List<String>): Map<String, JsonObject> {
        if (entityIds.isEmpty()) {
            return emptyMap()
        }

        val result = mutableMapOf<String, JsonObject>()
        val response = redisAPI.jsonMget(entityIds + "$").await()

        val collection = JsonArray(response.toString())

        for (item in collection) {
            if (item == null) continue

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
    override suspend fun findAllKeysForType(type: String): List<String> {
        val response = redisAPI.smembers("entity:types:$type").await()
        val uuids = response?.map { it.toString() } ?: emptyList()
        return uuids
    }

    /**
     * Finds multiple entities by their IDs and returns as JsonObjects
     * @param entityIds List of entity IDs to fetch
     * @return Map of entity IDs to JsonObject documents
     */
    override suspend fun findJson(entityIds: List<String>): Map<String, JsonObject> {
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
    override suspend fun findOneJson(entityId: String): JsonObject? {
        return findJsonByIds(listOf(entityId))[entityId]
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

                keys?.forEach { key ->
                    val keyStr = key.toString()
                    // Exclude frame delta keys
                    // TODO: Create a set of all entities so we don't need to scan and then remove this
                    if (!keyStr.startsWith("frame:")) {
                        entityKeys.add(keyStr)
                    }
                }
            } while (cursor != "0")

            entityKeys.sort() // Note this, very important

            return entityKeys
        } catch (e: Exception) {
            log.error("Error getting entity keys", e)
            return emptyList()
        }
    }

    /**
     *
     *  Lua
     *
     */

    /**
     * Atomic version increment
     */
    private fun versionIncrementScript(): String {
        return """
            redis.call('JSON.MERGE', KEYS[1], '${'$'}.state', ARGV[1])
            redis.call('JSON.NUMINCRBY', KEYS[1], '${'$'}.version', 1)
            local version = redis.call('JSON.GET', KEYS[1], 'version')
            return tonumber(version)
        """
    }

    /**
     * Batch merge entity deltas
     */
    private fun batchMergeDeltasScript(): String {
        return """
        for i = 1, #KEYS do
            redis.call('JSON.MERGE', KEYS[i], '${'$'}.state', ARGV[i])
            redis.call('JSON.NUMINCRBY', KEYS[i], '${'$'}.version', 1)
        end
    """
    }
}