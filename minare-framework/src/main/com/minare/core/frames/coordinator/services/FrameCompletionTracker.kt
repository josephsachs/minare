package com.minare.core.frames.coordinator.services

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.core.frames.services.WorkerRegistry
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Tracks frame completion status across workers using Hazelcast.
 * Updated for logical frames - uses frame numbers instead of timestamps.
 */
@Singleton
class FrameCompletionTracker @Inject constructor(
    private val hazelcastInstance: HazelcastInstance,
    private val workerRegistry: WorkerRegistry
) {
    private val log = LoggerFactory.getLogger(FrameCompletionTracker::class.java)
    private val debugTraceLogs: Boolean = false

    private val completionMap: IMap<String, JsonObject> by lazy {
        hazelcastInstance.getMap("frame-completions")
    }

    /**
     * Record that a worker has completed a logical frame.
     *
     * @param workerId The worker that completed
     * @param logicalFrame The logical frame number
     * @param operationCount Number of operations completed
     */
    fun recordWorkerCompletion(workerId: String, logicalFrame: Long, operationCount: Int) {
        val key = makeCompletionKey(logicalFrame, workerId)
        val completion = JsonObject()
            .put("workerId", workerId)
            .put("logicalFrame", logicalFrame)
            .put("operationCount", operationCount)
            .put("completedAt", System.currentTimeMillis())

        completionMap[key] = completion
        if (debugTraceLogs) {
            log.debug("Recorded completion for worker {} on logical frame {} ({} operations)",
                workerId, logicalFrame, operationCount)
        }
    }

    /**
     * Get all workers that have completed a specific logical frame.
     *
     * @param logicalFrame The logical frame to check
     * @return Set of worker IDs that have completed
     */
    fun getCompletedWorkers(logicalFrame: Long): Set<String> {
        val prefix = "frame-$logicalFrame:"

        return completionMap.keys
            .filter { it.startsWith(prefix) }
            .map { it.substringAfter(":") }
            .toSet()
    }

    /**
     * Get workers that haven't completed a logical frame yet.
     *
     * @param logicalFrame The logical frame to check
     * @return Set of worker IDs that haven't completed
     */
    fun getMissingWorkers(logicalFrame: Long): Set<String> {
        val completed = getCompletedWorkers(logicalFrame)
        val expected = workerRegistry.getActiveWorkers().toSet()  // Convert List to Set
        return expected - completed
    }

    /**
     * Clear completion records for a logical frame.
     * Should be called after frame is fully processed.
     *
     * @param logicalFrame The logical frame to clear
     */
    fun clearFrameCompletions(logicalFrame: Long) {
        val prefix = "frame-$logicalFrame:"

        val keysToRemove = completionMap.keys.filter { it.startsWith(prefix) }
        keysToRemove.forEach { completionMap.remove(it) }

        if (debugTraceLogs) {
            log.info("Cleared {} completion records for logical frame {}",
                keysToRemove.size, logicalFrame)
        }
    }

    /**
     * Check if all expected workers have completed a logical frame.
     *
     * @param logicalFrame The logical frame to check
     * @return true if all active workers have completed
     */
    fun isFrameComplete(logicalFrame: Long): Boolean {
        val completed = getCompletedWorkers(logicalFrame)
        val expected = workerRegistry.getActiveWorkers()

        return expected.isNotEmpty() && completed.containsAll(expected)
    }

    /**
     * Get completion statistics for a logical frame.
     *
     * @param logicalFrame The frame to analyze
     * @return JsonObject with completion statistics
     */
    fun getFrameCompletionStats(logicalFrame: Long): JsonObject {
        val completed = getCompletedWorkers(logicalFrame)
        val expected = workerRegistry.getActiveWorkers()
        val missing = expected - completed

        return JsonObject()
            .put("logicalFrame", logicalFrame)
            .put("expectedWorkers", expected.size)
            .put("completedWorkers", completed.size)
            .put("missingWorkers", missing.size)
            .put("completionPercentage",
                if (expected.isEmpty()) 100.0
                else (completed.size.toDouble() / expected.size) * 100)
            .put("completed", completed.toList())
            .put("missing", missing.toList())
    }

    /**
     * Clear ALL completion records from the distributed map.
     * Should be called when starting a new session to ensure clean state.
     */
    fun clearAllCompletions() {
        val keysToRemove = completionMap.keys.filter {
            it.startsWith("frame-") || it.startsWith("manifest:")
        }

        if (keysToRemove.isNotEmpty()) {
            keysToRemove.forEach { completionMap.remove(it) }
            if (debugTraceLogs) {
                log.debug("Cleared {} completion records from distributed map for new session", keysToRemove.size)
            }
        } else {
            if (debugTraceLogs) {
                log.info("No completion records to clear for new session")
            }
        }
    }

    /**
     * Create a completion map key for a logical frame and worker.
     */
    private fun makeCompletionKey(logicalFrame: Long, workerId: String): String {
        return "frame-$logicalFrame:$workerId"
    }
}