package com.minare.worker.coordinator

import com.minare.time.FrameConfiguration
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.*
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Shared state for the Frame Coordinator system.
 * Updated for event-driven coordination with monotonic time tracking.
 */
@Singleton
class FrameCoordinatorState @Inject constructor(
    private val workerRegistry: WorkerRegistry,
    private val frameConfig: FrameConfiguration
) {
    private val log = LoggerFactory.getLogger(FrameCoordinatorState::class.java)

    // Session timing state
    @Volatile
    var sessionStartTimestamp: Long = 0L
        private set

    @Volatile
    var sessionStartNanos: Long = 0L
        private set

    // Frame progress tracking
    private val _lastProcessedFrame = AtomicLong(-1L)
    private val _lastPreparedManifest = AtomicLong(-1L)
    private val _isPaused = AtomicBoolean(true)  // Start paused until workers ready

    // Operations buffered by logical frame number
    private val operationsByFrame = ConcurrentHashMap<Long, ConcurrentLinkedQueue<JsonObject>>()

    // Current frame completion tracking
    private val currentFrameCompletions = ConcurrentHashMap<String, Long>()
    private val _frameInProgress = AtomicLong(-1L)

    // Public properties
    var lastProcessedFrame: Long
        get() = _lastProcessedFrame.get()
        private set(value) = _lastProcessedFrame.set(value)

    var lastPreparedManifest: Long
        get() = _lastPreparedManifest.get()
        set(value) = _lastPreparedManifest.set(value)

    var isPaused: Boolean
        get() = _isPaused.get()
        set(value) = _isPaused.set(value)

    val frameInProgress: Long
        get() = _frameInProgress.get()

    /**
     * Start a new session with specific timestamps.
     * Called on startup or resume.
     *
     * @param startTimestamp Wall clock time for session start (for external communication)
     * @param startNanos Monotonic nanos for internal frame calculations
     */
    fun startNewSession(startTimestamp: Long = System.currentTimeMillis(),
                        startNanos: Long = System.nanoTime()) {
        sessionStartTimestamp = startTimestamp
        sessionStartNanos = startNanos
        _lastProcessedFrame.set(-1L)
        _lastPreparedManifest.set(-1L)
        _frameInProgress.set(-1L)
        operationsByFrame.clear()
        currentFrameCompletions.clear()

        setFrameInProgress(0)  // Start tracking frame 0

        log.info("Started new session at timestamp {} (nanos: {})",
            startTimestamp, startNanos)
    }

    /**
     * Buffer an operation for a specific logical frame
     */
    fun bufferOperation(operation: JsonObject, logicalFrame: Long) {
        operationsByFrame.computeIfAbsent(logicalFrame) {
            ConcurrentLinkedQueue()
        }.offer(operation)
    }

    /**
     * Get and remove all operations for a logical frame
     */
    fun extractFrameOperations(logicalFrame: Long): List<JsonObject> {
        val queue = operationsByFrame.remove(logicalFrame) ?: return emptyList()
        return queue.toList()  // Creates a snapshot
    }

    /**
     * Mark a logical frame as in progress
     */
    fun setFrameInProgress(logicalFrame: Long) {
        _frameInProgress.set(logicalFrame)
        currentFrameCompletions.clear()
    }

    /**
     * Record that a worker has completed the current frame
     */
    fun recordWorkerCompletion(workerId: String, logicalFrame: Long) {
        // Only record if it's for the current frame
        if (logicalFrame == _frameInProgress.get()) {
            currentFrameCompletions[workerId] = System.currentTimeMillis()
            log.debug("Worker {} completed logical frame {}", workerId, logicalFrame)
        } else {
            log.warn("Ignoring completion from worker {} for old frame {} (current: {})",
                workerId, logicalFrame, _frameInProgress.get())
        }
    }

    /**
     * Mark a frame as completely processed
     */
    fun markFrameProcessed(logicalFrame: Long) {
        _lastProcessedFrame.set(logicalFrame)
        log.debug("Marked logical frame {} as processed", logicalFrame)
    }

    /**
     * Get all workers that have completed the specified frame
     */
    fun getCompletedWorkers(logicalFrame: Long): Set<String> {
        return if (logicalFrame == _frameInProgress.get()) {
            currentFrameCompletions.keys.toSet()
        } else {
            emptySet()
        }
    }

    /**
     * Check if all expected workers have completed the current frame
     */
    fun isFrameComplete(logicalFrame: Long): Boolean {
        if (logicalFrame != _frameInProgress.get()) {
            return false
        }

        val completed = currentFrameCompletions.keys
        val expected = workerRegistry.getActiveWorkers()

        return completed.containsAll(expected)
    }

    /**
     * Get the logical frame for a given timestamp.
     * Uses monotonic time for accurate frame calculation.
     */
    fun getLogicalFrame(timestamp: Long): Long {
        if (sessionStartTimestamp == 0L) {
            throw IllegalStateException("Session not started")
        }

        val relativeTimestamp = timestamp - sessionStartTimestamp
        return if (relativeTimestamp < 0) {
            -1L // Before session start
        } else {
            relativeTimestamp / frameConfig.frameDurationMs
        }
    }

    /**
     * Get the current logical frame based on monotonic time.
     * More accurate than using wall clock time.
     */
    fun getCurrentLogicalFrame(): Long {
        if (sessionStartNanos == 0L) {
            return -1L
        }

        val elapsedNanos = System.nanoTime() - sessionStartNanos
        return elapsedNanos / (frameConfig.frameDurationMs * 1_000_000L)
    }

    /**
     * Get the wall clock time when a logical frame should start
     */
    fun getFrameStartTime(logicalFrame: Long): Long {
        if (sessionStartTimestamp == 0L) {
            throw IllegalStateException("Session not started")
        }
        return sessionStartTimestamp + (logicalFrame * frameConfig.frameDurationMs)
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
     * Get total buffered operations across all frames
     */
    fun getTotalBufferedOperations(): Int {
        return operationsByFrame.values.sumOf { it.size }
    }

    /**
     * Check if frame loop is running (for monitoring)
     */
    fun isFrameLoopRunning(): Boolean {
        return _frameInProgress.get() != -1L && !isPaused
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
        val currentFrame = if (sessionStartNanos != 0L) {
            getCurrentLogicalFrame()
        } else {
            -1L
        }

        return JsonObject()
            .put("frameInProgress", _frameInProgress.get())
            .put("lastProcessedFrame", _lastProcessedFrame.get())
            .put("lastPreparedManifest", _lastPreparedManifest.get())
            .put("currentWallClockFrame", currentFrame)
            .put("sessionStartTimestamp", sessionStartTimestamp)
            .put("completedWorkers", currentFrameCompletions.size)
            .put("totalWorkers", workerRegistry.getActiveWorkers().size)
            .put("isPaused", isPaused)
            .put("totalBufferedOperations", getTotalBufferedOperations())
    }

    /**
     * Check if we're approaching buffer limits during pause
     */
    fun isApproachingBufferLimit(): Boolean {
        if (!isPaused) return false

        val bufferedFrames = operationsByFrame.keys.maxOrNull()?.let { maxFrame ->
            maxFrame - _frameInProgress.get()
        } ?: 0

        return bufferedFrames >= frameConfig.maxBufferFrames - 2
    }

    /**
     * Reset all state (useful for testing or restart scenarios)
     */
    fun reset() {
        sessionStartTimestamp = 0L
        sessionStartNanos = 0L
        _lastProcessedFrame.set(-1L)
        _lastPreparedManifest.set(-1L)
        _frameInProgress.set(-1L)
        _isPaused.set(true)  // Reset to paused
        operationsByFrame.clear()
        currentFrameCompletions.clear()
        log.info("Frame coordinator state reset")
    }
}