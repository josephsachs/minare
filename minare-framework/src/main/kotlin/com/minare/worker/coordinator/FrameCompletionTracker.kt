package com.minare.worker.coordinator

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Tracks frame completion across workers.
 * Manages the distributed completion map and provides methods
 * to check frame progress and completion status.
 */
@Singleton
class FrameCompletionTracker @Inject constructor(
    private val hazelcastInstance: HazelcastInstance,
    private val coordinatorState: FrameCoordinatorState,
    private val workerRegistry: WorkerRegistry
) {
    private val log = LoggerFactory.getLogger(FrameCompletionTracker::class.java)

    private val completionMap: IMap<String, OperationCompletion> by lazy {
        hazelcastInstance.getMap("operation-completions")
    }

    /**
     * Record that a worker has completed its operations for a frame.
     * This updates both the coordinator state and the distributed map.
     *
     * @param workerId The ID of the worker
     * @param frameStartTime The frame start time
     * @param operationCount Number of operations completed
     */
    fun recordWorkerCompletion(
        workerId: String,
        frameStartTime: Long,
        operationCount: Int
    ) {
        // Update coordinator state
        coordinatorState.recordWorkerCompletion(workerId, frameStartTime)

        // Also track in distributed map for recovery purposes
        val completionKey = "frame-$frameStartTime:worker-$workerId"
        val completion = OperationCompletion(
            operationId = completionKey,
            workerId = workerId,
            completedAt = System.currentTimeMillis(),
            entityId = null,
            resultSummary = "frame:$frameStartTime,ops:$operationCount"
        )

        completionMap[completionKey] = completion

        log.debug("Recorded completion for worker {} on frame {} ({} operations)",
            workerId, frameStartTime, operationCount)
    }

    /**
     * Check if all expected workers have completed the current frame.
     *
     * @param frameStartTime The frame to check
     * @return true if all active workers have reported completion
     */
    fun isFrameComplete(frameStartTime: Long): Boolean {
        return coordinatorState.isFrameComplete(frameStartTime)
    }

    /**
     * Get the set of workers that have completed a specific frame.
     *
     * @param frameStartTime The frame to check
     * @return Set of worker IDs that have completed
     */
    fun getCompletedWorkers(frameStartTime: Long): Set<String> {
        return coordinatorState.getCompletedWorkers(frameStartTime)
    }

    /**
     * Get workers that haven't completed the frame yet.
     *
     * @param frameStartTime The frame to check
     * @return Set of worker IDs that haven't completed
     */
    fun getMissingWorkers(frameStartTime: Long): Set<String> {
        val completed = getCompletedWorkers(frameStartTime)
        val expected = workerRegistry.getActiveWorkers()
        return expected - completed
    }

    /**
     * Clear completion data for a frame.
     * Should be called after frame completion to free memory.
     *
     * @param frameStartTime The frame to clear
     */
    fun clearFrameCompletions(frameStartTime: Long) {
        // Clear from distributed map
        val completionKeys = completionMap.keys
            .filter { it.startsWith("frame-$frameStartTime:") }

        completionKeys.forEach { completionMap.remove(it) }

        log.debug("Cleared {} completions for frame {}",
            completionKeys.size, frameStartTime)
    }

    /**
     * Check if a specific operation was completed.
     * Used during recovery to identify incomplete operations.
     *
     * @param frameStartTime The frame time
     * @param operationId The operation ID to check
     * @return true if the operation was completed
     */
    fun isOperationCompleted(frameStartTime: Long, operationId: String): Boolean {
        val completionKey = "frame-$frameStartTime:op-$operationId"
        return completionMap.containsKey(completionKey)
    }

    /**
     * Record completion of a specific operation.
     * Used by workers to mark individual operations as complete.
     *
     * @param frameStartTime The frame time
     * @param operationId The operation ID
     * @param workerId The worker that completed it
     */
    fun recordOperationCompletion(
        frameStartTime: Long,
        operationId: String,
        workerId: String
    ) {
        val completionKey = "frame-$frameStartTime:op-$operationId"
        val completion = OperationCompletion(
            operationId = operationId,
            workerId = workerId,
            completedAt = System.currentTimeMillis()
        )

        completionMap[completionKey] = completion
    }

    /**
     * Get completion statistics for monitoring.
     *
     * @param frameStartTime The frame to analyze
     * @return JsonObject with completion statistics
     */
    fun getFrameCompletionStats(frameStartTime: Long): JsonObject {
        val completed = getCompletedWorkers(frameStartTime)
        val expected = workerRegistry.getActiveWorkers()
        val missing = expected - completed

        return JsonObject()
            .put("frameStartTime", frameStartTime)
            .put("expectedWorkers", expected.size)
            .put("completedWorkers", completed.size)
            .put("missingWorkers", missing.size)
            .put("completionPercentage",
                if (expected.isEmpty()) 100.0
                else (completed.size.toDouble() / expected.size) * 100)
            .put("completed", completed.toList())
            .put("missing", missing.toList())
    }
}