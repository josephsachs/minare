package com.minare.worker.coordinator

import com.minare.time.FrameConfiguration
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Shared state for the Frame Coordinator system.
 * Simplified to work with distributed maps for manifest and completion tracking.
 */
@Singleton
class FrameCoordinatorState @Inject constructor(
    private val workerRegistry: WorkerRegistry,
    private val frameConfig: FrameConfiguration
) {
    private val log = LoggerFactory.getLogger(FrameCoordinatorState::class.java)

    // Frame timing state
    private val _currentFrameStart = AtomicLong(0L)
    private val _isPaused = AtomicBoolean(false)

    // Operations buffered by frame start time
    private val operationsByFrame = ConcurrentHashMap<Long, MutableList<JsonObject>>()

    // Current frame completion tracking
    private val currentFrameCompletions = ConcurrentHashMap<String, Long>()
    private val _frameInProgress = AtomicLong(-1L)

    // Public properties
    var currentFrameStart: Long
        get() = _currentFrameStart.get()
        set(value) = _currentFrameStart.set(value)

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
     * Mark a frame as in progress
     */
    fun setFrameInProgress(frameStartTime: Long) {
        _frameInProgress.set(frameStartTime)
        currentFrameCompletions.clear()
    }

    /**
     * Record that a worker has completed the current frame
     */
    fun recordWorkerCompletion(workerId: String, frameStartTime: Long) {
        // Only record if it's for the current frame
        if (frameStartTime == _frameInProgress.get()) {
            currentFrameCompletions[workerId] = System.currentTimeMillis()
            log.debug("Worker {} completed frame {}", workerId, frameStartTime)
        } else {
            log.warn("Ignoring completion from worker {} for old frame {} (current: {})",
                workerId, frameStartTime, _frameInProgress.get())
        }
    }

    /**
     * Get all workers that have completed the current frame
     */
    fun getCompletedWorkers(frameStartTime: Long): Set<String> {
        return if (frameStartTime == _frameInProgress.get()) {
            currentFrameCompletions.keys.toSet()
        } else {
            emptySet()
        }
    }

    /**
     * Check if all expected workers have completed the current frame
     */
    fun isFrameComplete(frameStartTime: Long): Boolean {
        if (frameStartTime != _frameInProgress.get()) {
            return false
        }

        val completed = currentFrameCompletions.keys
        val expected = workerRegistry.getActiveWorkers()

        return completed.containsAll(expected)
    }

    /**
     * Get the frame start time for a given timestamp
     */
    fun getFrameStartTime(timestamp: Long): Long {
        val frameLength = frameConfig.frameDurationMs + frameConfig.frameOffsetMs
        return (timestamp / frameLength) * frameLength
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
     * Check if frame loop should be started based on current state
     */
    fun shouldStartFrameLoop(): Boolean {
        return !isPaused && workerRegistry.hasMinimumWorkers()
    }

    /**
     * Check if we have enough workers to process frames
     */
    fun hasMinimumWorkers(): Boolean {
        return workerRegistry.hasMinimumWorkers()
    }

    /**
     * Get buffered operation counts by frame (for monitoring)
     */
    fun getBufferedOperationCounts(): Map<Long, Int> {
        return operationsByFrame.mapValues { it.value.size }
    }

    /**
     * Check if frame loop is running (for monitoring)
     */
    fun isFrameLoopRunning(): Boolean {
        return _frameInProgress.get() != -1L
    }

    /**
     * Get frame completion status (for monitoring)
     */
    fun getFrameCompletionStatus(): Map<String, Long> {
        return currentFrameCompletions.toMap()
    }

    /**
     * Get current frame status (for monitoring)
     */
    fun getCurrentFrameStatus(): JsonObject {
        return JsonObject()
            .put("frameInProgress", _frameInProgress.get())
            .put("completedWorkers", currentFrameCompletions.size)
            .put("totalWorkers", workerRegistry.getActiveWorkers().size)
            .put("isPaused", isPaused)
    }

    /**
     * Reset all state (useful for testing or restart scenarios)
     */
    fun reset() {
        _currentFrameStart.set(0L)
        _frameInProgress.set(-1L)
        _isPaused.set(false)
        operationsByFrame.clear()
        currentFrameCompletions.clear()
        log.info("Frame coordinator state reset")
    }
}