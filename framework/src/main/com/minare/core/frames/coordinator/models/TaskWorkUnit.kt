package com.minare.core.frames.coordinator.models

import com.google.inject.Inject
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Task
import com.minare.core.entity.services.EntityObjectHydrator
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.work.WorkUnit
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.kotlinFunction

class TaskWorkUnit @Inject constructor(
): WorkUnit {
    @Inject private lateinit var reflectionCache: ReflectionCache
    @Inject private lateinit var objectHydrator: EntityObjectHydrator
    @Inject private lateinit var stateStore: StateStore

    /** @return Collection<String> of entity keys */
    override suspend fun prepare(): Collection<Any> {
        val entityTypes = reflectionCache.getJTypesHavingFunction<Task>()
        val allKeys = mutableListOf<String>()

        entityTypes.forEach { entityClass ->
            val typeName = entityClass.simpleName
            val keys = stateStore.findAllKeysForType(typeName)
            allKeys.addAll(keys)
        }

        return allKeys
    }

    /** @param items Collection<String> of entity keys */
    override suspend fun process(items: Collection<Any>): Any {
        val keys = items.map { it.toString() }

        // Batch fetch all entity JSONs from Redis
        val entityJsons = stateStore.findEntitiesJson(keys)

        entityJsons.forEach { (entityKey, entityJson) ->
            val entity = objectHydrator.hydrate(entityJson)

            // Get and invoke @Task methods
            val taskMethods = reflectionCache.getFunctionsWithAnnotation<Task>(entity.javaClass)

            taskMethods.forEach { method ->
                method.isAccessible = true
                val kFunction = method.kotlinFunction

                if (kFunction != null) {
                    if (kFunction.isSuspend) {
                        kFunction.callSuspend(entity)
                    } else {
                        kFunction.call(entity)
                    }
                }
            }
        }

        return Unit
    }
}