package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.worker.coordinator.FrameCoordinatorState
import com.minare.worker.coordinator.WorkerRegistry
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject

class WorkerRegisterEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry,
    private val frameCoordinatorState: FrameCoordinatorState
) {
    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_REGISTER) { message, traceId ->
            val workerId = message.body().getString("workerId")

            vlog.logStartupStep(
                "WORKER_REGISTER_REQUEST",
                mapOf("workerId" to workerId)
            )

            val registered = workerRegistry.registerWorker(workerId)

            if (registered) {
                vlog.getEventLogger().trace(
                    "WORKER_REGISTERED",
                    mapOf("workerId" to workerId),
                    traceId
                )

                // Check if we should start the frame loop
                if (frameCoordinatorState.shouldStartFrameLoop()) {
                    // Signal the coordinator verticle to start the frame loop
                    eventBusUtils.sendWithTracing<Unit>(
                        ADDRESS_START_FRAME_LOOP,
                        JsonObject().put("trigger", "worker_registered"),
                        traceId
                    )
                }
            } else {
                vlog.logVerticleError(
                    "WORKER_REGISTRATION_FAILED",
                    Exception(),
                    mapOf("workerId" to workerId)
                )
            }
        }
    }

    companion object {
        const val ADDRESS_WORKER_REGISTER = "minare.coordinator.worker.register"
        const val ADDRESS_START_FRAME_LOOP = "minare.coordinator.internal.start-frame-loop"
    }
}