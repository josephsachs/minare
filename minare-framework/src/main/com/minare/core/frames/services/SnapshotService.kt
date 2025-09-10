package com.minare.core.frames.services

import com.google.inject.Inject
import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.events.WorkerStateSnapshotCompleteEvent.Companion.ADDRESS_WORKER_STATE_SNAPSHOT_ALL_COMPLETE
import com.minare.core.storage.interfaces.DeltaStore
import com.minare.core.storage.interfaces.SnapshotStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.EventWaiter

class SnapshotService @Inject constructor(
    private val sessionMetadataReader: SessionMetadataReader,
    private val frameCoordinatorState: FrameCoordinatorState,
    private val snapshotStore: SnapshotStore,
    private val stateStore: StateStore,
    private val deltaStore: DeltaStore,
    private val eventBusUtils: EventBusUtils,
    private val eventWaiter: EventWaiter
) {

    suspend fun doSnapshot(sessionId: String) {
        // val metadata = sessionMetadataReader.getMetadata(sessionId)
        val deltas = deltaStore.getDeltasForFrameRange(0, frameCoordinatorState.lastPreparedManifest)
        snapshotStore.storeDeltas(sessionId, deltas)

        // val stateStore = countEntities()
        // val partitions = getEntityPartitions()
        // frameCoordinatorState.assignEntityPartitions(partitions)

        // eventBusUtils.publishWithTracing(
        //      ADDRESS_WORKER_START_STATE_SNAPSHOT,
        //      JsonObject().put("sessionId", sessionId),
        //      traceId
        // )

        eventWaiter.waitForEvent(ADDRESS_WORKER_STATE_SNAPSHOT_ALL_COMPLETE)

        // eventBusUtils.publishWithTracing(
        //      ADDRESS_SNAPSHOT_COMPLETE,
        //      JsonObject().put("sessionId", sessionId),
        //      traceId
        // )
    }
}