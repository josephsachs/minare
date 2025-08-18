package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.worker.coordinator.FrameCoordinatorState
import com.minare.worker.coordinator.FrameCoordinatorVerticle
import com.minare.worker.coordinator.WorkerRegistry
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import com.minare.worker.coordinator.FrameCompletionTracker
import io.vertx.core.json.JsonObject

/**
 * Handles frame completion events from workers.
 * Updated for logical frames - tracks completions by frame number.
 */
class WorkerFrameCompleteEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val coordinatorState: FrameCoordinatorState,
    private val workerRegistry: WorkerRegistry,
    private val frameCompletionTracker: FrameCompletionTracker
) {
    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_FRAME_COMPLETE) { message, traceId ->
            val workerId = message.body().getString("workerId")
            val logicalFrame = message.body().getLong("logicalFrame")  // Changed from frameStartTime
            val operationCount = message.body().getInteger("operationCount", 0)

            vlog.getEventLogger().trace(
                "FRAME_COMPLETE_RECEIVED",
                mapOf(
                    "workerId" to workerId,
                    "logicalFrame" to logicalFrame,
                    "operationCount" to operationCount
                ),
                traceId
            )

            // Validate worker is active
            if (!workerRegistry.isWorkerHealthy(workerId)) {
                vlog.logInfo("Ignoring completion from unhealthy worker $workerId")
                return@registerTracedConsumer
            }

            // Record completion
            coordinatorState.recordWorkerCompletion(workerId, logicalFrame)
            frameCompletionTracker.recordWorkerCompletion(workerId, logicalFrame, operationCount)

            // Check if all workers have completed this frame
            if (coordinatorState.isFrameComplete(logicalFrame)) {
                val completedWorkers = coordinatorState.getCompletedWorkers(logicalFrame)
                val activeWorkers = workerRegistry.getActiveWorkers()

                vlog.logInfo("Frame $logicalFrame progress: ${completedWorkers.size}/${activeWorkers.size} workers complete")

                // All workers complete - notify coordinator
                vlog.getEventLogger().trace(
                    "ALL_WORKERS_COMPLETE",
                    mapOf(
                        "logicalFrame" to logicalFrame,
                        "workerCount" to activeWorkers.size
                    ),
                    traceId
                )

                eventBusUtils.sendWithTracing(
                    FrameCoordinatorVerticle.ADDRESS_FRAME_ALL_COMPLETE,
                    JsonObject()
                        .put("logicalFrame", logicalFrame)
                        .put("completedWorkers", completedWorkers.size),
                    traceId
                )
            }
        }
    }

    companion object {
        const val ADDRESS_WORKER_FRAME_COMPLETE = "minare.coordinator.worker.frame.complete"
    }
}