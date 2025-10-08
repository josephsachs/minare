package com.minare.core.frames.coordinator

import com.google.inject.Inject
import com.minare.application.config.TaskConfiguration
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Task
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.models.Entity
import com.minare.core.frames.coordinator.models.TaskWorkUnit
import com.minare.core.frames.coordinator.services.StartupService
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.frames.worker.WorkerTaskVerticle
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.vertx.EventWaiter
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.work.WorkDispatchService
import io.vertx.core.Vertx
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch

class CoordinatorTaskVerticle @Inject constructor(
    private val taskConfiguration: TaskConfiguration,
    private val workDispatchService: WorkDispatchService,
    private val taskWorkUnit: TaskWorkUnit,
    private val startupService: StartupService,
    private val eventWaiter: EventWaiter,
    private val vlog: VerticleLogger,
    private val vertx: Vertx
): CoroutineVerticle() {
    private val log = LoggerFactory.getLogger(CoordinatorTaskVerticle::class.java)

    private var taskTimerId: Long = 0L
    private var isProcessing = false

    override suspend fun start() {
        log.info("Starting CoordinatorTaskVerticle")
        vlog.setVerticle(this)

        startupService.checkInitialWorkerStatus()

        //launch {
        log.info("Waiting for all workers to be ready...")
        startupService.awaitAllWorkersReady()
        log.info("All workers ready, starting session")
        startTaskLoop()
    }

    private suspend fun startTaskLoop() {
        taskTimerId = vertx.setPeriodic(taskConfiguration.msPerTick) {
            if (!isProcessing) {
                launch {
                    isProcessing = true
                    try {
                        processTick()
                    } finally {
                        isProcessing = false
                    }
                }
            }
        }
    }

    private suspend fun processTick() {
        workDispatchService.dispatch(
            "entity.tasks.tick",
            WorkDispatchService.Companion.WorkDispatchStrategy.RANGE,
            taskWorkUnit
        )

        eventWaiter.waitForEvent("${WorkDispatchService.ADDRESS_WORK_COMPLETE_EVENT}.entity.tasks.tick")
    }

    override suspend fun stop() {
        log.info("Stopping CoordinatorTaskVerticle")
        super.stop()
    }
}