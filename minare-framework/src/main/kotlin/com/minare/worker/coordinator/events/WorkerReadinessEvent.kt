package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import com.minare.worker.coordinator.FrameCoordinatorState
import com.minare.worker.coordinator.FrameCoordinatorVerticle
import com.minare.worker.coordinator.WorkerRegistry
import io.vertx.core.json.JsonObject

/**
 * WorkerReadinessEvent triggers when a worker announces activation, causes FrameCoordinatorVerticle
 * to check if we're ready to begin frame loop. Coordinator will also check status in tryStartSession().
 */
class WorkerReadinessEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry,
    private val coordinatorState: FrameCoordinatorState
) {
    suspend fun register() {
        // Listen to successful worker activations only
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_ACTIVATED) { message, traceId ->
            val workerId = message.body().getString("workerId")
            val activeWorkers = message.body().getInteger("activeWorkers")
            val expectedWorkers = message.body().getInteger("expectedWorkers")

            vlog.logInfo("Worker activated: ${workerId}. Status: ${activeWorkers}/${expectedWorkers} active")

            // Only proceed if no session is running and all workers are ready
            if (activeWorkers == expectedWorkers &&
                coordinatorState.sessionStartTimestamp == 0L) {

                vlog.getEventLogger().trace(
                    "ALL_WORKERS_READY",
                    mapOf(
                        "activeWorkers" to activeWorkers,
                        "expectedWorkers" to expectedWorkers
                    ),
                    traceId
                )

                // Notify coordinator that all workers are ready
                eventBusUtils.sendWithTracing(
                    FrameCoordinatorVerticle.ADDRESS_ALL_WORKERS_READY,
                    JsonObject()
                        .put("activeWorkers", activeWorkers)
                        .put("expectedWorkers", expectedWorkers),
                    traceId
                )
            }
        }
    }

    companion object {
        const val ADDRESS_WORKER_ACTIVATED = "minare.coordinator.worker.activated"
    }
}