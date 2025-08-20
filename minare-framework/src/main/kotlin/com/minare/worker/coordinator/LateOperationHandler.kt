package com.minare.worker.coordinator

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
    fun handleLateOperation(
        operation: JsonObject,
        intendedFrame: Long,
        currentFrame: Long
    ): LateOperationDecision
}

/**
 * Decision on how to handle a late operation
 */
sealed class LateOperationDecision {
    object Drop : LateOperationDecision()
    data class Delay(val targetFrame: Long) : LateOperationDecision()
}

/**
 * Reject strategy - drops late operations and logs them.
 * This is the simplest and most deterministic approach.
 */
class RejectLateOperations(
    private val maxFramesLate: Int = 0  // For logging/metrics purposes
) : LateOperationHandler {
    private val log = LoggerFactory.getLogger(RejectLateOperations::class.java)

    override fun handleLateOperation(
        operation: JsonObject,
        intendedFrame: Long,
        currentFrame: Long
    ): LateOperationDecision {
        val framesLate = currentFrame - intendedFrame

        log.warn("Dropping late operation {} intended for frame {} (current: {}, {} frames late)",
            operation.getString("id"),
            intendedFrame,
            currentFrame,
            framesLate
        )

        return LateOperationDecision.Drop
    }
}

/**
 * Delay strategy - adds late operations to the next unprocessed frame.
 * Less deterministic but more forgiving of network delays.
 */
class DelayLateOperations(
    private val maxDelayFrames: Int = 5
) : LateOperationHandler {
    private val log = LoggerFactory.getLogger(DelayLateOperations::class.java)

    override fun handleLateOperation(
        operation: JsonObject,
        intendedFrame: Long,
        currentFrame: Long
    ): LateOperationDecision {
        val nextFrame = currentFrame + 1
        val delayedFrames = nextFrame - intendedFrame

        if (delayedFrames > maxDelayFrames) {
            log.warn("Operation {} too late to delay ({} frames), dropping",
                operation.getString("id"),
                delayedFrames
            )
            return LateOperationDecision.Drop
        }

        log.info("Delaying operation {} from frame {} to frame {}",
            operation.getString("id"),
            intendedFrame,
            nextFrame
        )

        return LateOperationDecision.Delay(nextFrame)
    }
}