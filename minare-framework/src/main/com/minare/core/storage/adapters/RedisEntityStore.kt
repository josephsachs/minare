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

@Singleton
class RedisEntityStore @Inject constructor(
    private val redisAPI: RedisAPI,
    private val reflectionCache: ReflectionCache,
    private val entityFactory: EntityFactory,
    private val publishService: EntityPublishService
) : StateStore {

    private val log = LoggerFactory.getLogger(RedisEntityStore::class.java)

    override suspend fun save(entity: Entity): Entity {
        val stateJson = JsonObject()

        val entityType = entity.type
        if (entityType != null) {
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
        }

        // Build full document
        val document = JsonObject()
            .put("_id", entity._id)
            .put("type", entity.type)
            .put("version", entity.version ?: 1)
            .put("state", stateJson)

        // Store in Redis using entity ID as key
        redisAPI.jsonSet(listOf(entity._id!!, "$", document.encode())).await()

        return entity
    }

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

    override suspend fun findEntitiesByIds(entityIds: List<String>): Map<String, Entity> {
        if (entityIds.isEmpty()) {
            return emptyMap()
        }

        val result = mutableMapOf<String, Entity>()

        for (entityId in entityIds) {
            try {
                val entityJson = redisAPI.jsonGet(listOf(entityId, "$")).await()

                if (entityJson != null) {
                    val document = JsonObject(entityJson.toString())
                    val entityType = document.getString("type")
                    val version = document.getLong("version", 1L)

                    if (entityType != null) {
                        val entity = entityFactory.getNew(entityType).apply {
                            this._id = entityId
                            this.version = version
                            this.type = entityType
                        }

                        result[entityId] = entity
                    }
                }
            } catch (e: Exception) {
                // Continue with other entities
                log.warn("Error fetching entity $entityId: ${e.message}")
            }
        }

        return result
    }

    override suspend fun findEntitiesWithState(entityIds: List<String>): Map<String, Entity> {
        if (entityIds.isEmpty()) {
            return emptyMap()
        }

        val result = mutableMapOf<String, Entity>()

        for (entityId in entityIds) {
            try {
                val entityJson = redisAPI.jsonGet(listOf(entityId, "$")).await()

                if (entityJson != null) {
                    val document = JsonObject(entityJson.toString())
                    val entityType = document.getString("type")
                    val version = document.getLong("version", 1L)
                    val state = document.getJsonObject("state", JsonObject())

                    if (entityType != null) {
                        val entity = entityFactory.getNew(entityType).apply {
                            this._id = entityId
                            this.version = version
                            this.type = entityType
                        }

                        // Populate state fields using reflection
                        entityFactory.useClass(entityType)?.let { entityClass ->
                            val stateFields = reflectionCache.getFieldsWithAnnotation<State>(entityClass)
                            for (field in stateFields) {
                                field.isAccessible = true
                                try {
                                    val value = state.getValue(field.name)
                                    if (value != null) {
                                        field.set(entity, value)
                                    }
                                } catch (e: Exception) {
                                    // Log error but continue with other fields
                                }
                            }
                        }

                        result[entityId] = entity
                    }
                }
            } catch (e: Exception) {
                // Log error but continue with other entities
                log.warn("Error fetching entity with state $entityId: ${e.message}")
            }
        }

        return result
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

        for (entityId in entityIds) {
            try {
                val redisResponse = redisAPI.jsonGet(listOf(entityId, "$")).await()

                if (redisResponse != null) {
                    // Parse as JsonArray since Redis always returns an array
                    val jsonArray = JsonArray(redisResponse.toString())

                    // Extract entities from the array
                    for (i in 0 until jsonArray.size()) {
                        val entityJson = jsonArray.getJsonObject(i)
                        // Get the ID from the entity JSON if possible, otherwise use the requested ID
                        val id = entityJson.getString("_id") ?: entityId
                        result[id] = entityJson
                    }
                }
            } catch (e: Exception) {
                log.warn("Error fetching entity JSON for $entityId: ${e.message}")
                // Continue with other entities
            }
        }

        return result
    }

    /**
     * Finds an entity by ID and returns it as an Entity
     * @param entityId The ID of the entity to fetch
     * @return The entity as a JsonObject, or null if not found
     */
    override suspend fun findEntity(entityId: String): Entity? {
        return findEntitiesByIds(listOf(entityId))[entityId]
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