package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.worker.coordinator.WorkerRegistry
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject

class WorkerHeartbeatEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry
) {
    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_HEARTBEAT) { message, traceId ->
            val workerId = message.body().getString("workerId")
            workerRegistry.updateHeartbeat(workerId)
        }
    }

    companion object {
        const val ADDRESS_WORKER_HEARTBEAT = "minare.coordinator.worker.heartbeat"
    }
}