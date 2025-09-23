package com.minare.core.frames.events

import com.google.inject.Inject
import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.events.WorkerStateSnapshotCompleteEvent.Companion.ADDRESS_WORKER_STATE_SNAPSHOT_COMPLETE
import com.minare.core.storage.interfaces.SnapshotStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.vertx.EventBusUtils
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

class WorkerStartStateSnapshotEvent @Inject constructor(
    private val coordinatorState: FrameCoordinatorState,
    private val stateStore: StateStore,
    private val snapshotStore: SnapshotStore,
    private val eventBusUtils: EventBusUtils
) {
    private val log = LoggerFactory.getLogger(WorkerStartStateSnapshotEvent::class.java)

    fun register(workerId: String) {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_START_STATE_SNAPSHOT) { message, traceId ->
            val sessionId = message.body().getString("sessionId")

            log.info("SNAPSHOT: Getting partitions for snapshot $sessionId")

            //val entityIds = coordinatorState.getEntityPartition(workerId)

            //log.info("SNAPSHOT: got partitions for $workerId containing ${entityIds}")

            /**if (entityIds.isNotEmpty()) {
                log.info("SNAPSHOT: getting entity IDs")

                val entities = stateStore.findEntitiesJsonByIds(entityIds).values.toList()

                log.info("SNAPSHOT: got entityIds, storing...")

                snapshotStore.storeState(sessionId, entities)

                log.info("SNAPSHOT: Stored...")
            }**/

            log.info("SNAPSHOT: Publishing completion event for $workerId")

            eventBusUtils.publishWithTracing(
                ADDRESS_WORKER_STATE_SNAPSHOT_COMPLETE,
                JsonObject()
                    .put("workerId", workerId)
                    .put("sessionId", sessionId),
                traceId
            )
        }
    }

    companion object {
        const val ADDRESS_WORKER_START_STATE_SNAPSHOT = "minare.coordinator.worker.state.snapshot.start"
    }
}