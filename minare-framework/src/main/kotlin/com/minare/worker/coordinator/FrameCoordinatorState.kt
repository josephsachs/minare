package com.minare.worker.coordinator

import com.minare.time.FrameConfiguration
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Shared state for the Frame Coordinator system.
 * This allows event handlers and other components to interact with coordinator state
 * without needing direct access to the verticle.
 */
@Singleton
class FrameCoordinatorState @Inject constructor(
    private val workerRegistry: WorkerRegistry,
    private val frameConfig: FrameConfiguration
) {
    private val log = LoggerFactory.getLogger(FrameCoordinatorState::class.java)

    // Frame timing state
    private val _currentFrameStart = AtomicLong(0L)
    private val _nextFrameTimerId = AtomicReference<Long?>(null)
    private val _isPaused = AtomicBoolean(false)

    // Operations buffered by frame start time
    private val operationsByFrame = ConcurrentHashMap<Long, MutableList<JsonObject>>()

    // Frame assignments for tracking and recovery
    private val frameAssignments = ConcurrentHashMap<Long, Map<String, List<JsonObject>>>()

    // Completion tracking by frame start time
    private val frameCompletions = ConcurrentHashMap<String, FrameCompletion>()

    // Public properties
    var currentFrameStart: Long
        get() = _currentFrameStart.get()
        set(value) = _currentFrameStart.set(value)

    var nextFrameTimerId: Long?
        get() = _nextFrameTimerId.get()
        set(value) = _nextFrameTimerId.set(value)

    var isPaused: Boolean
        get() = _isPaused.get()
        set(value) = _isPaused.set(value)

    /**
     * Buffer an operation for a specific frame
     */
    fun bufferOperation(operation: JsonObject, frameStartTime: Long) {
        operationsByFrame.computeIfAbsent(frameStartTime) {
            mutableListOf()
        }.add(operation)
    }

    /**
     * Get and remove all operations for a frame
     */
    fun extractFrameOperations(frameStartTime: Long): List<JsonObject> {
        return operationsByFrame.remove(frameStartTime) ?: emptyList()
    }

    /**
     * Store frame assignments for recovery purposes
     */
    fun storeFrameAssignments(frameStartTime: Long, assignments: Map<String, List<JsonObject>>) {
        frameAssignments[frameStartTime] = assignments
    }

    /**
     * Get frame assignments for recovery
     */
    fun getFrameAssignments(frameStartTime: Long): Map<String, List<JsonObject>>? {
        return frameAssignments[frameStartTime]
    }

    /**
     * Remove frame assignments after successful completion
     */
    fun clearFrameAssignments(frameStartTime: Long) {
        frameAssignments.remove(frameStartTime)
    }

    /**
     * Record a frame completion from a worker
     */
    fun recordFrameCompletion(completion: FrameCompletion) {
        frameCompletions[completion.workerId] = completion
        log.debug("Recorded frame completion from worker {} for frame {}",
            completion.workerId, completion.frameStartTime)
    }

    /**
     * Get all workers that have completed a specific frame
     */
    fun getFrameCompletions(frameStartTime: Long): Set<String> {
        return frameCompletions.values
            .filter { it.frameStartTime == frameStartTime }
            .map { it.workerId }
            .toSet()
    }

    /**
     * Clear all frame completions (usually at start of new frame)
     */
    fun clearFrameCompletions() {
        frameCompletions.clear()
    }

    /**
     * Get incomplete operations for specific workers from a frame
     */
    fun getIncompleteOperations(frameStartTime: Long, workerIds: Collection<String>): List<JsonObject> {
        return frameAssignments[frameStartTime]
            ?.filterKeys { it in workerIds }
            ?.values
            ?.flatten()
            ?: emptyList()
    }

    /**
     * Check if frame loop is running
     */
    fun isFrameLoopRunning(): Boolean {
        return nextFrameTimerId != null
    }

    /**
     * Get all buffered operations (for monitoring)
     */
    fun getBufferedOperationCounts(): Map<Long, Int> {
        return operationsByFrame.mapValues { it.value.size }
    }

    /**
     * Get frame completion status for monitoring
     */
    fun getFrameCompletionStatus(): Map<String, FrameCompletion> {
        return frameCompletions.toMap()
    }

    /**
     * Check if frame loop should be started based on current state
     */
    fun shouldStartFrameLoop(): Boolean {
        return !isPaused &&
                !isFrameLoopRunning() &&
                workerRegistry.hasMinimumWorkers()
    }

    /**
     * Calculate the next frame start time aligned to frame boundaries
     */
    fun calculateNextFrameStart(): Long {
        val now = System.currentTimeMillis()
        val frameLength = frameConfig.frameDurationMs + frameConfig.frameOffsetMs
        return ((now / frameLength) + 1) * frameLength
    }

    /**
     * Get the frame start time for a given timestamp
     */
    fun getFrameStartTime(timestamp: Long): Long {
        val frameLength = frameConfig.frameDurationMs + frameConfig.frameOffsetMs
        return (timestamp / frameLength) * frameLength
    }

    /**
     * Calculate the frame deadline for completion tracking
     */
    fun calculateFrameDeadline(frameEndTime: Long): Long {
        return frameEndTime +
                (frameConfig.frameOffsetMs * frameConfig.coordinationWaitPeriod).toLong()
    }

    /**
     * Check if we have enough workers to process frames
     */
    fun hasMinimumWorkers(): Boolean {
        return workerRegistry.hasMinimumWorkers()
    }

    /**
     * Reset all state (useful for testing or restart scenarios)
     */
    fun reset() {
        _currentFrameStart.set(0L)
        _nextFrameTimerId.set(null)
        _isPaused.set(false)
        operationsByFrame.clear()
        frameAssignments.clear()
        frameCompletions.clear()
        log.info("Frame coordinator state reset")
    }
}

/**
 * Data class for frame completion tracking
 */
data class FrameCompletion(
    val workerId: String,
    val frameStartTime: Long,
    val operationCount: Int,
    val completedAt: Long
)