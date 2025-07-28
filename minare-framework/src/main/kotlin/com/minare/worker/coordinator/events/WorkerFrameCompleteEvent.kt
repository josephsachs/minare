package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.worker.coordinator.FrameCoordinatorState
import com.minare.worker.coordinator.FrameCoordinatorVerticle
import com.minare.worker.coordinator.WorkerRegistry
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject

/**
 * Handles frame completion events from workers.
 * Now checks if all workers are complete and triggers the next frame.
 */
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
            frameCoordinatorState.recordWorkerCompletion(workerId, frameStartTime)
            workerRegistry.recordFrameCompletion(workerId, frameStartTime)

            // Log completion
            vlog.getEventLogger().trace(
                "WORKER_FRAME_COMPLETED",
                mapOf(
                    "workerId" to workerId,
                    "frameStartTime" to frameStartTime,
                    "operationCount" to operationCount
                ),
                traceId
            )

            // Check if all workers have completed this frame
            if (frameCoordinatorState.isFrameComplete(frameStartTime)) {
                vlog.logInfo("All workers completed frame $frameStartTime")

                // Signal the coordinator that the frame is complete
                eventBusUtils.sendWithTracing(
                    FrameCoordinatorVerticle.ADDRESS_FRAME_ALL_COMPLETE,
                    JsonObject().put("frameStartTime", frameStartTime),
                    traceId
                )

                vlog.getEventLogger().trace(
                    "FRAME_ALL_WORKERS_COMPLETE",
                    mapOf(
                        "frameStartTime" to frameStartTime,
                        "completedWorkers" to frameCoordinatorState.getCompletedWorkers(frameStartTime).size
                    ),
                    traceId
                )
            } else {
                // Log progress
                val completed = frameCoordinatorState.getCompletedWorkers(frameStartTime)
                val total = workerRegistry.getActiveWorkers().size
                vlog.logInfo("Frame $frameStartTime progress: ${completed.size}/$total workers complete")
            }
        }
    }

    companion object {
        const val ADDRESS_WORKER_FRAME_COMPLETE = "minare.coordinator.worker.frame.complete"
    }
}