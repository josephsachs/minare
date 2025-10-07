package com.minare.core.frames.coordinator

import com.google.inject.Inject
import com.minare.application.config.TaskConfiguration
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Task
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.models.Entity
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.Vertx
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch

class CoordinatorTaskVerticle @Inject constructor(
    private val taskConfiguration: TaskConfiguration,
    private val entityFactory: EntityFactory,
    private val reflectionCache: ReflectionCache,
    private val vlog: VerticleLogger,
    private val vertx: Vertx
): CoroutineVerticle() {
    private val log = LoggerFactory.getLogger(CoordinatorTaskVerticle::class.java)

    private var taskTimerId: Long = 0L

    override suspend fun start() {
        log.info("Starting CoordinatorTaskVerticle")
        vlog.setVerticle(this)

        taskTimerId = vertx.setPeriodic(taskConfiguration.msPerTick) {
            launch {
                processTick()
            }
        }
    }

    private suspend fun processTick() {
        val entityTypes = reflectionCache.getJTypesHavingFunction<Task>()

        entityTypes.forEach({ type ->
            val instance = entityFactory.createEntity(type)
            val taskMethods = reflectionCache.getFunctionsWithAnnotation<Task>(type)
            taskMethods.forEach { method ->
                method.isAccessible = true
                method.invoke(instance)
            }
        })
    }

    override suspend fun stop() {
        log.info("Stopping CoordinatorTaskVerticle")
        super.stop()
    }
}