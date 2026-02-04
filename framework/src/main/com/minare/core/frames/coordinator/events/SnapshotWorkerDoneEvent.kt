package com.minare.core.frames.coordinator.events

import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.json.JsonObject
import com.google.inject.Inject

class SnapshotCompletionEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry
) {
    private val completedWorkers = mutableSetOf<String>()

    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_SNAPSHOT_WORKER_DONE) { message, traceId ->
            val workerId = message.body().getString("workerId")
            val verification = message.body().getJsonObject("verification")

            completedWorkers.add(workerId)

            vlog.logInfo("Snapshot completed by worker $workerId: ${completedWorkers.size}/${workerRegistry.getActiveWorkers().size}")

            if (completedWorkers.size == workerRegistry.getActiveWorkers().size) {
                vlog.logInfo("All workers completed snapshot")

                // Notify coordinator that snapshot is complete
                eventBusUtils.sendWithTracing(
                    ADDRESS_SNAPSHOT_ALL_COMPLETE,
                    JsonObject().put("completedWorkers", completedWorkers.size),
                    traceId
                )

                // Reset for next snapshot
                completedWorkers.clear()
            }
        }
    }

    companion object {
        const val ADDRESS_SNAPSHOT_WORKER_DONE = "minare.snapshot.worker.done"
        const val ADDRESS_SNAPSHOT_ALL_COMPLETE = "minare.snapshot.all.complete"
    }
}