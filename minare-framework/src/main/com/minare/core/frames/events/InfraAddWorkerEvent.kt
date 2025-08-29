package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.core.frames.services.WorkerRegistry
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject

/**
 * InfraAddWorkerEvent occurs when CoordinatorAdminVerticle receives add worker command
 */
class InfraAddWorkerEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry
) {
    fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_INFRA_ADD_WORKER) { message, traceId ->
            val workerId = message.body().getString("workerId")

            vlog.logStartupStep(
                "INFRA_ADD_WORKER_REQUEST",
                mapOf("workerId" to workerId)
            )

            try {
                workerRegistry.addWorker(workerId)

                vlog.getEventLogger().trace(
                    "WORKER_ADDED",
                    mapOf("workerId" to workerId),
                    traceId
                )

                eventBusUtils.tracedReply(
                    message,
                    JsonObject().put("success", true),
                    traceId
                )
            } catch (e: Exception) {
                vlog.logVerticleError(
                    "INFRA_ADD_WORKER",
                    e,
                    mapOf("workerId" to workerId)
                )
                message.fail(500, e.message ?: "Error adding worker")
            }
        }
    }

    companion object {
        const val ADDRESS_INFRA_ADD_WORKER = "minare.coordinator.infra.add-worker"
    }
}