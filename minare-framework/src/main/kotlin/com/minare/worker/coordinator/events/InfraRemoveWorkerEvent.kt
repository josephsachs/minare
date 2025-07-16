package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.coordinator.WorkerRegistry
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.coordinator.FrameCoordinatorVerticle

class InfraRemoveWorkerEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry
) {
    fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_INFRA_REMOVE_WORKER) { message, traceId ->
            val workerId = message.body().getString("workerId")

            vlog.logStartupStep(
                "INFRA_REMOVE_WORKER_REQUEST",
                mapOf("workerId" to workerId)
            )

            try {
                workerRegistry.scheduleWorkerRemoval(workerId)

                vlog.getEventLogger().trace(
                    "WORKER_REMOVED",
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
                    "INFRA_REMOVE_WORKER",
                    e,
                    mapOf("workerId" to workerId)
                )
                message.fail(500, e.message ?: "Error scheduling worker removal")
            }
        }
    }

    companion object {
        const val ADDRESS_INFRA_REMOVE_WORKER = "minare.coordinator.infra.remove-worker"
    }
}