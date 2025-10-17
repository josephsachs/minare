package com.minare.core.frames.coordinator.services

import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.coordinator.handlers.LateOperationHandler
import com.minare.core.utils.debug.OperationDebugUtils
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Handles Kafka consumption for the frame coordinator.
 * Responsible for setting up the Kafka consumer and buffering operations
 * into the appropriate logical frames.
 *
 * Updated with event-driven manifest preparation and backpressure handling.
 */
@Singleton
class OperationHandler @Inject constructor(
    private val coordinatorState: FrameCoordinatorState,
    private val manifestBuilder: FrameManifestBuilder,
    private val lateOperationHandler: LateOperationHandler,
    private val operationDebugUtils: OperationDebugUtils // TEMPORARY DEBUG
) {
    private val log = LoggerFactory.getLogger(OperationHandler::class.java)
    private val debugTraceLogs: Boolean = false

    /**
     * Process a single operation, checking buffer limits and routing to frames.
     * Updated to properly enforce frame-based buffer limits.
     *
     * Note: This method does not control Kafka commits. We always commit to avoid
     * duplicate operations. Atomicity and idempotency must be handled at the operation level.
     *
     * @return true if processing should continue, false if backpressure was activated
     */
    fun handle(operation: JsonObject): Boolean {
        val timestamp = operation.getLong("timestamp")
        val logicalFrame = coordinatorState.getLogicalFrame(timestamp)
        val frameInProgress = coordinatorState.frameInProgress

        if (debugTraceLogs) {
            operationDebugUtils.logOperation(operation, "OperationHandler.handle(operation): frameInProgress = $frameInProgress - logicalFrame = $logicalFrame - timestamp = $timestamp")
        }

        if (logicalFrame <= frameInProgress) {
            lateOperationHandler.handle(operation)
            return true
        }

        if (logicalFrame <= coordinatorState.lastPreparedManifest) {
            manifestBuilder.assignToExistingManifest(operation, logicalFrame)
        } else {
            coordinatorState.bufferOperation(operation, logicalFrame)
        }

        return true
    }

    /**
     * Get all the operations we buffered during pause
     */
    suspend fun extractBuffered(): MutableMap<Long, List<JsonObject>> {
        val oldFrameNumbers = coordinatorState.getBufferedOperationCounts().keys.sorted()
        val operationsByOldFrame = mutableMapOf<Long, List<JsonObject>>()

        oldFrameNumbers.forEach { oldFrame ->
            val extracted = coordinatorState.extractFrameOperations(oldFrame)

            if (debugTraceLogs) {
                operationDebugUtils.logOperation(extracted, "OperationHandler.extractBuffered(): oldFrame = $oldFrame")
            }

            operationsByOldFrame[oldFrame] = extracted
        }

        if (debugTraceLogs) {
            log.info("Extracted operations from old frames: ${operationsByOldFrame.entries}")
        }

        return operationsByOldFrame
    }

    /**
     * Re-buffer operations with new frame numbers, preserving groupings
     */
    suspend fun assignBuffered(operationsByFrame: MutableMap<Long, List<JsonObject>>): Long {
        var count = 0L

        operationsByFrame.forEach { (_, operations) ->
            operations.forEach { op ->
                val timestamp = op.getLong("timestamp")
                val calculatedFrame = coordinatorState.getLogicalFrame(timestamp)

                if (calculatedFrame < 0) {
                    if (debugTraceLogs) {
                        operationDebugUtils.logOperation(op, "OperationHandler.assignBuffered() handle - calculatedFrame = $calculatedFrame - timestamp = $timestamp")
                    }

                    // If the calculated frame is negative, then it was in flight during a session transition
                    // and is treated as a late operation
                    lateOperationHandler.handle(op)
                } else {
                    if (debugTraceLogs) {
                        operationDebugUtils.logOperation(op, "OperationHandler.assignBuffered() bufferOperation - calculatedFrame = $calculatedFrame - timestamp = $timestamp")
                    }

                    coordinatorState.bufferOperation(op, calculatedFrame)
                }
            }

            count++
        }

        return count
    }
}