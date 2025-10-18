package com.minare.core.frames.coordinator.services

import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.coordinator.handlers.LateOperationHandler
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.Type
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
    private val debug: DebugLogger
) {

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

        debug.log(Type.COORDINATOR_OPERATION_HANDLER_HANDLE, listOf(operation.toString(), frameInProgress, timestamp))

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

            debug.log(Type.COORDINATOR_OPERATION_HANDLER_EXTRACT_BUFFERED, listOf(oldFrame))

            operationsByOldFrame[oldFrame] = extracted
        }

        debug.log(Type.COORDINATOR_OPERATION_HANDLER_EXTRACTED_OPS, listOf(operationsByOldFrame.size))

        return operationsByOldFrame
    }

    /**
     * Re-buffer operations with new frame numbers, preserving groupings
     */
    suspend fun assignBuffered(operationsByFrame: MutableMap<Long, List<JsonObject>>): Long {
        var count = 0L

        operationsByFrame.forEach { (_, operations) ->
            operations.forEach { op ->
                val jsonString = operations.toString()
                val timestamp = op.getLong("timestamp")
                val calculatedFrame = coordinatorState.getLogicalFrame(timestamp)

                if (calculatedFrame < 0) {
                    debug.log(Type.COORDINATOR_OPERATION_HANDLER_ASSIGN_LATE_OPERATION, listOf(jsonString, calculatedFrame, timestamp))

                    // If the calculated frame is negative, then it was in flight during a session transition
                    // and is treated as a late operation
                    lateOperationHandler.handle(op)
                } else {
                    debug.log(Type.COORDINATOR_OPERATION_HANDLER_ASSIGN_OPERATION, listOf(jsonString, calculatedFrame, timestamp))

                    coordinatorState.bufferOperation(op, calculatedFrame)
                }
            }

            count++
        }

        return count
    }
}