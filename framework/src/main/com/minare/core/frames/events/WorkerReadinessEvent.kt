package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.core.frames.coordinator.services.StartupService
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.json.JsonObject

/**
 * WorkerReadinessEvent triggers when a worker announces activation.
 * Delegates to StartupService to track readiness and coordinate session start.
 */
class WorkerReadinessEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val startupService: StartupService
) {
    suspend fun register() {
        // Listen to successful worker activations
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_ACTIVATED) { message, traceId ->
            val workerId = message.body().getString("workerId")
            val activeWorkers = message.body().getInteger("activeWorkers")
            val expectedWorkers = message.body().getInteger("expectedWorkers")

            vlog.logInfo("Worker activated: ${workerId}. Status: ${activeWorkers}/${expectedWorkers} active")

            // Delegate to StartupService to track readiness
            startupService.handleWorkerReady(workerId)
        }
    }

    companion object {
        const val ADDRESS_WORKER_ACTIVATED = "minare.coordinator.worker.activated"
    }
}