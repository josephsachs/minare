package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.MinareApplication.ConnectionEvents.ADDRESS_WORKER_STARTED
import com.minare.worker.coordinator.FrameCoordinatorState
import com.minare.worker.coordinator.WorkerRegistry
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject

/**
 * Handles the ADDRESS_WORKER_STARTED event from MinareApplication.
 * This event indicates that a worker has fully deployed all its verticles
 * and is ready to participate in frame processing.
 *
 * Repurposed from handling custom registration messages to using the
 * existing worker startup signal.
 */
class WorkerRegisterEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry,
    private val frameCoordinatorState: FrameCoordinatorState
) {
    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_STARTED) { message, traceId ->
            val workerId = message.body().getString("workerId")

            vlog.logStartupStep(
                "WORKER_STARTED_RECEIVED",
                mapOf("workerId" to workerId)
            )

            // Transition worker from PENDING to ACTIVE
            val activated = workerRegistry.activateWorker(workerId)

            if (activated) {
                vlog.getEventLogger().trace(
                    "WORKER_ACTIVATED",
                    mapOf(
                        "workerId" to workerId,
                        "status" to "ACTIVE"
                    ),
                    traceId
                )

                // Log activation success
                vlog.logStartupStep(
                    "WORKER_ACTIVATION_COMPLETE",
                    mapOf(
                        "workerId" to workerId,
                        "activeWorkers" to workerRegistry.getActiveWorkers().size
                    )
                )
            } else {
                vlog.logVerticleError(
                    "WORKER_ACTIVATION_FAILED",
                    Exception("Worker not in PENDING state or not found"),
                    mapOf("workerId" to workerId)
                )
            }
        }
    }

    companion object {
        // No internal constants needed
    }
}