package com.minare.core.frames.services

import com.google.inject.Inject
import com.minare.core.storage.interfaces.DeltaStore
import com.minare.core.storage.interfaces.SnapshotStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.vertx.EventBusUtils
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory

class SnapshotService @Inject constructor(
    private val snapshotStore: SnapshotStore,
    private val stateStore: StateStore,
    private val deltaStore: DeltaStore,
    private val eventBusUtils: EventBusUtils
) {
    private val log = LoggerFactory.getLogger(SnapshotService::class.java)

    suspend fun doSnapshot(sessionId: String, scope: CoroutineScope) {
        // Capture current state from Redis before the new session can mutate it
        val deltas = deltaStore.getAll()
        val entities = stateStore.getAllEntityKeys()
        val entitiesMap = stateStore.findJsonByIds(entities)
        deltaStore.clearDeltas()

        // Fire-and-forget: persist to Mongo in background
        scope.launch {
            try {
                snapshotStore.storeDeltas(sessionId, deltas)
                snapshotStore.storeState(sessionId, JsonArray(entitiesMap.values.toList()))
            } catch (e: Exception) {
                log.error("Background snapshot persistence failed for session: $sessionId", e)
            }
        }

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