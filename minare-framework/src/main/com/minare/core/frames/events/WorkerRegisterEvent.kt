package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.core.MinareApplication.ConnectionEvents.ADDRESS_WORKER_STARTED
import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.json.JsonObject

/**
 * Handles the ADDRESS_WORKER_STARTED event from MinareApplication.
 * This event indicates that a worker has fully deployed all its verticles
 * and is ready to participate in frame processing.
 *
 * Enforces strict worker registration - only pre-registered workers
 * via infrastructure commands are allowed to join the cluster.
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

            // Check if worker has been pre-registered via infrastructure
            val workerState = workerRegistry.getWorkerState(workerId)
            if (workerState == null) {
                // Reject unregistered workers to maintain deterministic operation distribution
                vlog.logInfo("Rejecting worker $workerId - not pre-registered in infrastructure")

                vlog.getEventLogger().trace(
                    "WORKER_REJECTED",
                    mapOf(
                        "workerId" to workerId,
                        "reason" to "Not pre-registered"
                    ),
                    traceId
                )

                // Log for monitoring/alerting
                vlog.logStartupStep(
                    "UNREGISTERED_WORKER_REJECTED",
                    mapOf(
                        "workerId" to workerId,
                        "activeWorkers" to workerRegistry.getActiveWorkers().size,
                        "expectedWorkers" to workerRegistry.getExpectedWorkerCount()
                    )
                )

                return@registerTracedConsumer
            }

            // Worker is pre-registered, proceed with activation
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

                // Emit event for other components to react to successful activation
                eventBusUtils.sendWithTracing(
                    ADDRESS_WORKER_ACTIVATED,
                    JsonObject()
                        .put("workerId", workerId)
                        .put("activeWorkers", workerRegistry.getActiveWorkers().size)
                        .put("expectedWorkers", workerRegistry.getExpectedWorkerCount()),
                    traceId
                )


                // Check if we now have all expected workers
                val activeCount = workerRegistry.getActiveWorkers().size
                val expectedCount = workerRegistry.getExpectedWorkerCount()

                if (activeCount == expectedCount) {
                    vlog.logInfo("All expected workers now active: $activeCount/$expectedCount")
                }

            } else {
                // Worker couldn't be activated (e.g., in REMOVING state)
                val currentStatus = workerRegistry.getWorkerState(workerId)?.status?.toString() ?: "UNKNOWN"

                vlog.logInfo("Worker $workerId could not be activated: $currentStatus")

                vlog.getEventLogger().trace(
                    "WORKER_ACTIVATION_FAILED",
                    mapOf(
                        "workerId" to workerId,
                        "currentStatus" to currentStatus
                    ),
                    traceId
                )
            }
        }
    }

    companion object {
        const val ADDRESS_WORKER_ACTIVATED = "minare.coordinator.worker.activated"
    }
}