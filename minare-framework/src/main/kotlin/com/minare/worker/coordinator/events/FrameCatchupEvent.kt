package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import com.minare.worker.coordinator.FrameCompletionTracker
import com.minare.worker.coordinator.FrameCoordinatorState
import com.minare.worker.coordinator.FrameCoordinatorVerticle.Companion.ADDRESS_WORKERS_CAUGHT_UP
import com.minare.worker.coordinator.WorkerRegistry
import io.vertx.core.json.JsonObject

/**
 * FrameCatchupEvent handles frame catchup when returning from paused state
 *
 * TODO: Re-implement with pause and recover strategies
 */
class FrameCatchUpEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val coordinatorState: FrameCoordinatorState,
    private val frameCompletionTracker: FrameCompletionTracker,
    private val workerRegistry: WorkerRegistry
) {
    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_FRAME_COMPLETE) { message, traceId ->
            if (!coordinatorState.isPaused) {
                return@registerTracedConsumer
            }

            val workerId = message.body().getString("workerId")
            val logicalFrame = message.body().getLong("logicalFrame")
            val targetFrame = coordinatorState.lastProcessedFrame

            if (logicalFrame == targetFrame && frameCompletionTracker.isFrameComplete(targetFrame)) {
                val activeWorkers = workerRegistry.getActiveWorkers()
                val completedWorkers = frameCompletionTracker.getCompletedWorkers(targetFrame)

                vlog.logInfo("Catch-up progress for frame ${targetFrame}: ${completedWorkers.size}/${activeWorkers.size} workers complete",)

                vlog.getEventLogger().trace(
                    "WORKERS_CAUGHT_UP",
                    mapOf(
                        "targetFrame" to targetFrame,
                        "completedWorkers" to completedWorkers.size,
                        "activeWorkers" to activeWorkers.size
                    ),
                    traceId
                )

                eventBusUtils.sendWithTracing(
                    ADDRESS_WORKERS_CAUGHT_UP,
                    JsonObject()
                        .put("lastProcessedFrame", targetFrame)
                        .put("resumeFrame", targetFrame + 1)
                        .put("completedWorkers", completedWorkers.toList())
                        .put("activeWorkers", activeWorkers),
                    traceId
                )
            }
        }
    }

    companion object {
        const val ADDRESS_WORKER_FRAME_COMPLETE = "minare.coordinator.worker.frame.complete"
    }
}