package com.minare.core.frames.coordinator.models

import com.google.inject.Inject
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Task
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.work.WorkUnit
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

class TaskWorkUnit @Inject constructor(
): WorkUnit {
    @Inject private lateinit var reflectionCache: ReflectionCache
    @Inject private lateinit var entityFactory: EntityFactory
    @Inject private lateinit var stateStore: StateStore

    private var log = LoggerFactory.getLogger(TaskWorkUnit::class.java)

    /** @return Collection<String> of entity keys */
    override suspend fun prepare(): Collection<Any> {
        val entityTypes = reflectionCache.getJTypesHavingFunction<Task>()
        val allKeys = mutableListOf<String>()

        // TEMPORARY DEBUG
        log.info("TURN_CONTROLLER: CoordinatorTaskVerticle work unit preparing")

        entityTypes.forEach { entityClass ->
            val typeName = entityClass.simpleName
            val keys = stateStore.findKeysByType(typeName)
            allKeys.addAll(keys)
        }

        return allKeys
    }

    /** @param items Collection<String> of entity keys */
    override suspend fun process(items: Collection<Any>): Any {
        val workerId = System.getenv("HOSTNAME") ?: "unknown"

        // TEMPORARY DEBUG
        log.info("TURN_CONTROLLER: CoordinatorTaskVerticle work unit processing")

        val keys = items.map { it.toString() }

        // Batch fetch all entity JSONs from Redis
        val entityJsons = stateStore.findEntitiesJson(keys)

        entityJsons.forEach { (entityKey, entityJson) ->
            val entityType = entityJson.getString("type")
            val entityClass = entityFactory.useClass(entityType) ?: return@forEach

            val entity = entityFactory.createEntity(entityClass).apply {
                _id = entityJson.getString("_id")
                version = entityJson.getLong("version")
                type = entityType
            }

            val stateJson = entityJson.getJsonObject("state", JsonObject())
            stateStore.setEntityState(entity, entityType, stateJson)

            // Get and invoke @Task methods
            val taskMethods = reflectionCache.getFunctionsWithAnnotation<Task>(entityClass)

            taskMethods.forEach { method ->
                method.isAccessible = true
                method.invoke(entity, workerId)
            }
        }

        return Unit
    }
}