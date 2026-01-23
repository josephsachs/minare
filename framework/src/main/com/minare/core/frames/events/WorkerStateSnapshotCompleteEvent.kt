package com.minare.core.frames.events

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.utils.vertx.EventBusUtils
import io.vertx.core.json.JsonObject

@Singleton
class WorkerStateSnapshotCompleteEvent @Inject constructor(
    private val workerRegistry: WorkerRegistry,
    private val eventBusUtils: EventBusUtils
) {
    private val completedWorkers = mutableSetOf<String>()

    fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_STATE_SNAPSHOT_COMPLETE) { message, traceId ->
            val workerId = message.body().getString("workerId")
            val sessionId = message.body().getString("sessionId")
            completedWorkers.add(workerId)

            if (completedWorkers.size == workerRegistry.getActiveWorkers().size) {
                eventBusUtils.publishWithTracing(
                    ADDRESS_WORKER_STATE_SNAPSHOT_ALL_COMPLETE,
                    JsonObject().put("sessionId", sessionId),
                    traceId
                )

                clearCompletedWorkers()
            }
        }
    }

    private fun clearCompletedWorkers() {
        completedWorkers.clear()
    }

    companion object {
        const val ADDRESS_WORKER_STATE_SNAPSHOT_COMPLETE = "minare.coordinator.worker.state.snapshot.complete"
        const val ADDRESS_WORKER_STATE_SNAPSHOT_ALL_COMPLETE = "minare.coordinator.worker.state.snapshot.all-complete"
    }
}