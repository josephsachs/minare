package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.worker.coordinator.FrameCompletion
import com.minare.worker.coordinator.FrameCoordinatorState
import com.minare.worker.coordinator.WorkerRegistry
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject

class WorkerFrameCompleteEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry,
    private val frameCoordinatorState: FrameCoordinatorState
) {
    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_FRAME_COMPLETE) { message, traceId ->
            val workerId = message.body().getString("workerId")
            val frameStartTime = message.body().getLong("frameStartTime")
            val operationCount = message.body().getInteger("operationCount", 0)

            // Check if we should process this
            if (frameCoordinatorState.isPaused) {
                vlog.logInfo("Ignoring frame completion while paused")
                return@registerTracedConsumer
            }

            // Record the completion
            val completion = FrameCompletion(
                workerId = workerId,
                frameStartTime = frameStartTime,
                operationCount = operationCount,
                completedAt = System.currentTimeMillis()
            )

            frameCoordinatorState.recordFrameCompletion(completion)
            workerRegistry.recordFrameCompletion(workerId, frameStartTime)

            // Log it
            vlog.getEventLogger().trace(
                "FRAME_COMPLETION_RECORDED",
                mapOf(
                    "workerId" to workerId,
                    "frameStartTime" to frameStartTime,
                    "operationCount" to operationCount
                ),
                traceId
            )
        }
    }

    companion object {
        const val ADDRESS_WORKER_FRAME_COMPLETE = "minare.coordinator.worker.frame.complete"
    }
}