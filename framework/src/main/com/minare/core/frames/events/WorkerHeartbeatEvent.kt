package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.utils.vertx.EventBusUtils
import io.vertx.core.json.JsonObject

/**
 * WorkerHeartbeatEvent handles worker heartbeat events
 */
class WorkerHeartbeatEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
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