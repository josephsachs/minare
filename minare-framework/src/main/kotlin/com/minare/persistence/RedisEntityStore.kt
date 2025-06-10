package com.minare.persistence

import com.google.inject.Singleton
import com.minare.controller.EntityController
import com.minare.core.entity.EntityFactory
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.State
import com.minare.core.models.Entity
import com.minare.entity.EntityPublishService
import com.minare.utils.JsonFieldConverter
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.RedisAPI
import org.slf4j.LoggerFactory
import javax.inject.Inject

@Singleton
class RedisEntityStore @Inject constructor(
    private val redisAPI: RedisAPI,
    private val reflectionCache: ReflectionCache,
    private val entityFactory: EntityFactory,
    private val publishService: EntityPublishService
) : StateStore {
    private val log = LoggerFactory.getLogger(EntityController::class.java)

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

    override suspend fun mutateState(entityId: String, delta: JsonObject): JsonObject {
        // Read current entity from Redis
        val currentJson = redisAPI.jsonGet(listOf(entityId, "$")).await()

        if (currentJson == null) {
            throw IllegalStateException("Entity not found: $entityId")
        }

        val currentDocument = JsonObject(currentJson.toString())
        val currentState = currentDocument.getJsonObject("state", JsonObject())

        // Apply delta to state
        delta.fieldNames().forEach { fieldName ->
            currentState.put(fieldName, delta.getValue(fieldName))
        }

        // Increment version
        val newVersion = currentDocument.getLong("version", 1L) + 1
        currentDocument.put("version", newVersion)
        currentDocument.put("state", currentState)

        // Store updated document
        redisAPI.jsonSet(listOf(entityId, "$", currentDocument.encode())).await()

        // Publish the change
        publishService.publishStateChange(entityId, currentDocument.getString("type"), newVersion, delta)

        return currentDocument
    }

    override suspend fun findEntitiesByIds(entityIds: List<String>): Map<String, Entity> {
        if (entityIds.isEmpty()) return emptyMap()

        return entityIds.mapNotNull { entityId ->
            try {
                redisAPI.jsonGet(listOf(entityId, "$")).await()
                    ?.let { JsonArray(it.toString()) }
                    ?.mapNotNull { arrayElement ->
                        (arrayElement as? JsonObject)?.let { document ->
                            createEntityFromDocument(entityId, document)
                        }
                    }
                    ?.associateBy { it._id!! }
            } catch (e: Exception) {
                log.error("Failed to process entityId=$entityId", e)
                null
            }
        }
        .filterNotNull()
        .fold(mutableMapOf<String, Entity>()) { acc, entityMap ->
            acc.putAll(entityMap)
            acc
        }
    }

    private fun createEntityFromDocument(entityId: String, document: JsonObject): Entity? {
        val entityType = document.getString("type") ?: return null
        val version = document.getLong("version", 1L)
        val state = document.getJsonObject("state", JsonObject())

        return entityFactory.getNew(entityType).apply {
            this._id = entityId
            this.version = version
            this.type = entityType
        }.also { entity ->
            entityFactory.useClass(entityType)?.let { entityClass ->
                val jsonFieldConverter = JsonFieldConverter()
                reflectionCache.getFieldsWithAnnotation<State>(entityClass)
                    .forEach { field ->
                        field.isAccessible = true
                        state.getValue(field.name)?.let { value ->
                            try {
                                val convertedValue = jsonFieldConverter.convert(field, value)
                                field.set(entity, convertedValue)
                            } catch (e: Exception) {
                                log.error("Failed to convert field ${field.name} for entity $entityId: ${e.message}")
                            }
                        }
                    }
            }
        }
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
                            val stateFields = reflectionCache.getFieldsWithAnnotation<com.minare.core.entity.annotations.State>(entityClass)
                            for (field in stateFields) {
                                field.isAccessible = true
                                try {
                                    val value = state.getValue(field.name)
                                    if (value != null) {
                                        field.set(entity, value)
                                    }
                                } catch (e: Exception) {
                                    val temp = "trash"
                                    // Log error but continue with other fields
                                }
                            }
                        }

                        result[entityId] = entity
                    }
                }
            } catch (e: Exception) {
                // Log error but continue with other entities
            }
        }

        return result
    }
}