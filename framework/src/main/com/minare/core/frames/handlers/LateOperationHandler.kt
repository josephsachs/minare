package com.minare.core.frames.coordinator.handlers

import com.google.inject.Inject
import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.coordinator.services.FrameManifestBuilder
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

/**
 * Strategy interface for handling operations that arrive too late
 * to be included in their intended logical frame.
 *
 * Note that a late operation is defined by the relationship of its Kafka timestamp to the
 * logical frame schedule, not the time of consumption.In other words, here we are handling
 * client-to-worker latency, not divergence between frame coordinator and worker cluster.
 */
interface LateOperationHandler {
    /**
     * Handle a late operation.
     *
     * @param operation The late operation
     * @param intendedFrame The logical frame it was meant for
     * @param currentFrame The current logical frame being processed
     * @return LateOperationDecision indicating what to do
     */
    fun handle(
        operation: JsonObject
    )
}

/**
 * Reject strategy - drops late operations and logs them.
 * This is the simplest and most deterministic approach.
 */

class RejectLateOperation @Inject constructor(
    private val coordinatorState: FrameCoordinatorState
) : LateOperationHandler {
    private val log = LoggerFactory.getLogger(RejectLateOperation::class.java)

    override fun handle(
        operation: JsonObject
    ) {
        log.info("Rejected late operation ${operation.getString("id")} for frame ${coordinatorState.frameInProgress}")
    }
}

/**
 * Delay strategy - adds late operations to the next unprocessed frame.
 * Less deterministic but more forgiving of network delays.
 */
class DelayLateOperation @Inject constructor(
    private val coordinatorState: FrameCoordinatorState,
    private val manifestBuilder: FrameManifestBuilder
) : LateOperationHandler {
    private val log = LoggerFactory.getLogger(DelayLateOperation::class.java)

    override fun handle(
        operation: JsonObject
    ) {
        val timestamp = operation.getLong("timestamp")
        val logicalFrame = coordinatorState.getLogicalFrame(timestamp)

        val nextFrame = coordinatorState.frameInProgress + 1

        if (nextFrame <= coordinatorState.lastPreparedManifest) {
            manifestBuilder.assignToExistingManifest(operation, nextFrame)
        } else {
            coordinatorState.bufferOperation(operation, nextFrame)
        }

        log.info("Delaying operation {} from frame {} to frame {}",
            operation.getString("id"),
            logicalFrame,
            nextFrame
        )
    }
}