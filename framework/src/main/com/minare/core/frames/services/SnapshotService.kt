package com.minare.core.frames.services

import com.google.inject.Inject
import com.minare.core.storage.interfaces.DeltaStore
import com.minare.core.storage.interfaces.SnapshotStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.vertx.EventBusUtils
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

class SnapshotService @Inject constructor(
    private val snapshotStore: SnapshotStore,
    private val stateStore: StateStore,
    private val deltaStore: DeltaStore,
    private val eventBusUtils: EventBusUtils
) {
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

    companion object {
        const val ADDRESS_SNAPSHOT_COMPLETE = "minare.coordinator.worker.snapshot.complete"

        enum class SnapshotStoreOption {
            MONGO,
            JSON,
            NONE
        }
    }
}