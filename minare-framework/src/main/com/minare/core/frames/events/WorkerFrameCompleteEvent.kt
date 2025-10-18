package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.frames.coordinator.services.FrameCompletionTracker
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.utils.debug.DebugLogger
import com.minare.exceptions.FrameLoopException
import io.vertx.core.json.JsonObject

/**
 * Handles frame completion events from workers, allowing coordinator to give go-ahead to progress
 */
class WorkerFrameCompleteEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val debug: DebugLogger,
    private val coordinatorState: FrameCoordinatorState,
    private val workerRegistry: WorkerRegistry,
    private val frameCompletionTracker: FrameCompletionTracker
) {
    suspend fun register(debugTraceLogs: Boolean = false) {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_FRAME_COMPLETE) { message, traceId ->
            val workerId = message.body().getString("workerId")
            val logicalFrame = message.body().getLong("logicalFrame")  // Changed from frameStartTime
            val operationCount = message.body().getInteger("operationCount", 0)

            if (debugTraceLogs) {
                vlog.getEventLogger().trace(
                    "FRAME_COMPLETE_RECEIVED",
                    mapOf(
                        "workerId" to workerId,
                        "logicalFrame" to logicalFrame,
                        "operationCount" to operationCount
                    ),
                    traceId
                )
            }

            if (!workerRegistry.isWorkerHealthy(workerId)) {
                // TODO: Probable recovery trigger
                return@registerTracedConsumer
            }

            coordinatorState.recordWorkerCompletion(workerId, logicalFrame)
            frameCompletionTracker.recordWorkerCompletion(workerId, logicalFrame, operationCount)

            if (coordinatorState.isFrameComplete(logicalFrame)) {
                val completedWorkers = coordinatorState.getCompletedWorkers(logicalFrame)
                val activeWorkers = workerRegistry.getActiveWorkers()
                val lastPreparedManifest = coordinatorState.lastPreparedManifest
                val frameInProgress = coordinatorState.frameInProgress
                val completed = completedWorkers.size
                val active = activeWorkers.size

                debug.log(DebugLogger.Companion.Type.COORDINATOR_WORKER_FRAME_COMPLETE_EVENT, listOf(vlog, completed, active, logicalFrame, traceId))

                if (frameInProgress == lastPreparedManifest)  {
                    eventBusUtils.publishWithTracing(
                        FrameCoordinatorVerticle.ADDRESS_FRAME_MANIFESTS_ALL_COMPLETE,
                        JsonObject(),
                        traceId
                    )
                }

                if (frameInProgress > lastPreparedManifest) {
                    throw FrameLoopException("Frame in progress is ahead of last prepared manifest")
                }

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