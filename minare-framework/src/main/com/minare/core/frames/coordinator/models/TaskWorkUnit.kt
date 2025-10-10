package com.minare.core.frames.coordinator.models

import com.google.inject.Inject
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Task
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.work.WorkUnit
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.kotlinFunction

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

        log.info("TURN_CONTROLLER: $workerId received ${items.toString()}}")

        val keys = items.map { it.toString() }

        log.info("TURN_CONTROLLER: TaskWorkUnit process Next 1")

        // Batch fetch all entity JSONs from Redis
        val entityJsons = stateStore.findEntitiesJson(keys)

        log.info("TURN_CONTROLLER: TaskWorkUnit process Next 2")

        entityJsons.forEach { (entityKey, entityJson) ->
            log.info("TURN_CONTROLLER: TaskWorkUnit process Next 3")

            val entityType = entityJson.getString("type")
            val entityClass = entityFactory.useClass(entityType) ?: return@forEach

            log.info("TURN_CONTROLLER: TaskWorkUnit process Next 4")

            val entity = entityFactory.createEntity(entityClass).apply {
                _id = entityJson.getString("_id")
                version = entityJson.getLong("version")
                type = entityType
            }

            log.info("TURN_CONTROLLER: TaskWorkUnit process Next 5")

            val stateJson = entityJson.getJsonObject("state", JsonObject())

            log.info("TURN_CONTROLLER: TaskWorkUnit about to sppend state with" +
                    "$entityType")
            log.info("${entityClass.simpleName}")
            log.info("${entity._id}")
            log.info("${entity.type}")
            log.info("${entity.version}")
            log.info("${stateJson}")

            stateStore.setEntityState(entity, entityType, stateJson)

            log.info("TURN_CONTROLLER: TaskWorkUnit process Next 6")

            // Get and invoke @Task methods
            val taskMethods = reflectionCache.getFunctionsWithAnnotation<Task>(entityClass)

            log.info("TURN_CONTROLLER: TaskWorkUnit process Next 7 with ${taskMethods}")

            taskMethods.forEach { method ->
                method.isAccessible = true
                method.kotlinFunction?.callSuspend(entity)
            }
        }

        return Unit
    }
}