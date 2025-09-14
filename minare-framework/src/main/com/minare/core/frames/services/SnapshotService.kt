package com.minare.core.frames.services

import com.google.inject.Inject
import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.events.WorkerStartStateSnapshotEvent.Companion.ADDRESS_WORKER_START_STATE_SNAPSHOT
import com.minare.core.frames.events.WorkerStateSnapshotCompleteEvent.Companion.ADDRESS_WORKER_STATE_SNAPSHOT_ALL_COMPLETE
import com.minare.core.storage.interfaces.DeltaStore
import com.minare.core.storage.interfaces.SnapshotStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.EventWaiter
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

class SnapshotService @Inject constructor(
    private val snapshotStore: SnapshotStore,
    private val workerRegistry: WorkerRegistry,
    private val stateStore: StateStore,
    private val deltaStore: DeltaStore,
    private val eventBusUtils: EventBusUtils
) {
    private val log = LoggerFactory.getLogger(SnapshotService::class.java)

    suspend fun doSnapshot(sessionId: String) {
        val deltas = deltaStore.getAll()

        snapshotStore.storeDeltas(sessionId, deltas)
        deltaStore.clearDeltas()

        val entities = stateStore.getAllEntityKeys()
        val entitiesMap = stateStore.findEntitiesJsonByIds(entities)

        snapshotStore.storeState(
            sessionId,
            JsonArray(entitiesMap.values.toList())
            )

        eventBusUtils.publishWithTracing(
              ADDRESS_SNAPSHOT_COMPLETE,
              JsonObject().put("sessionId", sessionId)
       )
    }

    /**
     * Partitions entities for workers. Deprecated in favor of upcoming manifest service.
     */
    suspend fun getEntityPartitions(entities: List<String>): Map<String, List<String>> {
        val workers = workerRegistry.getActiveWorkers()

        if (workers.isEmpty() || entities.isEmpty()) {
            return emptyMap()
        }

        val partitions = mutableMapOf<String, List<String>>()
        val entitiesPerWorker = entities.size / workers.size
        val remainder = entities.size % workers.size

        var currentIndex = 0

        workers.forEachIndexed { index, workerId ->
            // Give the first 'remainder' workers one extra entity
            val extra = if (index < remainder) 1 else 0
            val partitionSize = entitiesPerWorker + extra

            // Take this worker's slice of entities
            val endIndex = minOf(currentIndex + partitionSize, entities.size)
            partitions[workerId] = entities.subList(currentIndex, endIndex)

            currentIndex = endIndex
        }

        return partitions
    }

    companion object {
        const val ADDRESS_SNAPSHOT_COMPLETE = "minare.coordinator.worker.snapshot.complete"
    }
}