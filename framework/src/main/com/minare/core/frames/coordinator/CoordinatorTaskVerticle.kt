package com.minare.core.frames.coordinator

import com.google.inject.Inject
import com.minare.application.config.FrameworkConfig
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle.Companion.ADDRESS_NEXT_FRAME
import com.minare.core.frames.coordinator.models.FixedTaskWorkUnit
import com.minare.core.frames.coordinator.models.TaskWorkUnit
import com.minare.core.utils.vertx.EventWaiter
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.work.WorkDispatchService
import io.vertx.core.Vertx
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch

class CoordinatorTaskVerticle @Inject constructor(
    private val frameworkConfig: FrameworkConfig,
    private val workDispatchService: WorkDispatchService,
    private val taskWorkUnit: TaskWorkUnit,
    private val fixedTaskWorkUnit: FixedTaskWorkUnit,
    private val eventWaiter: EventWaiter,
    private val vlog: VerticleLogger,
    private val vertx: Vertx
): CoroutineVerticle() {
    private val log = LoggerFactory.getLogger(CoordinatorTaskVerticle::class.java)

    private var taskTimerId: Long = 0L
    private var isTaskProcessing = false
    private var isFixedTaskProcesing = false

    override suspend fun start() {
        log.info("Starting CoordinatorTaskVerticle")
        vlog.setVerticle(this)
        registerFixedTaskEvent()
        startTaskLoop()
    }

    private suspend fun startTaskLoop() {
        log.info("CoordinatorTaskVerticle starting task process")

        taskTimerId = vertx.setPeriodic(frameworkConfig.tasks.tickInterval) {
            if (!isTaskProcessing) {
                launch {
                    isTaskProcessing = true
                    try {
                        processTick()
                    } finally {
                        isTaskProcessing = false
                    }
                }
            }
        }
    }

    private suspend fun registerFixedTaskEvent() {
        vertx.eventBus().consumer<JsonObject>(ADDRESS_NEXT_FRAME, { msg ->
            if (!isFixedTaskProcesing) {
                launch {
                    isFixedTaskProcesing = true
                    try {
                        processFixedTick()
                    } finally {
                        isFixedTaskProcesing = false
                    }
                }
            }
        })
    }

    private suspend fun processTick() {
        workDispatchService.dispatch(
            "entity.tasks.tick",
            WorkDispatchService.Companion.WorkDispatchStrategy.RANGE,
            taskWorkUnit
        )

        eventWaiter.waitFor("${WorkDispatchService.ADDRESS_WORK_COMPLETE_EVENT}.entity.tasks.tick")
    }

    private suspend fun processFixedTick() {
        workDispatchService.dispatch(
            "entity.tasks.fixed.tick",
            WorkDispatchService.Companion.WorkDispatchStrategy.RANGE,
            fixedTaskWorkUnit
        )

        eventWaiter.waitFor("${WorkDispatchService.ADDRESS_WORK_COMPLETE_EVENT}.entity.tasks.fixed.tick")
    }

    override suspend fun stop() {
        log.info("Stopping CoordinatorTaskVerticle")
        super.stop()
    }
}