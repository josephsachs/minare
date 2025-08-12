package com.minare.worker.coordinator

import com.minare.time.FrameConfiguration
import com.minare.time.FrameCalculator
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
    private val frameCalculator: FrameCalculator,
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

    // Operations buffered before session starts
    private val pendingOperations = ConcurrentLinkedQueue<JsonObject>()

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

        return expected.isNotEmpty() && completed.containsAll(expected)  // Added empty check
    }

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
        // Note: pendingOperations are NOT cleared - they'll be moved to frame 0

        setFrameInProgress(0)  // Start tracking frame 0

        log.info("Started new session at timestamp {} (nanos: {})",
            startTimestamp, startNanos)
    }

    /**
     * Buffer an operation before session starts.
     * These will all go into frame 0 when the session begins.
     */
    fun bufferPendingOperation(operation: JsonObject) {
        pendingOperations.offer(operation)
    }

    /**
     * Get count of pending operations waiting for session start.
     */
    fun getPendingOperationCount(): Int = pendingOperations.size

    /**
     * Move all pending operations to frame 0.
     * Called when starting a new session.
     */
    fun assignPendingOperationsToFrame(targetFrame: Long) {
        val frameQueue = operationsByFrame.computeIfAbsent(targetFrame) {
            ConcurrentLinkedQueue()
        }

        var count = 0
        while (true) {
            val op = pendingOperations.poll() ?: break
            frameQueue.offer(op)
            count++
        }

        log.info("Moved {} pending operations to frame {}", count, targetFrame)
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
        return queue.toList()
    }

    /**
     * Set the frame currently in progress
     */
    fun setFrameInProgress(frameNumber: Long) {
        _frameInProgress.set(frameNumber)
        currentFrameCompletions.clear()
    }

    /**
     * Mark a frame as processed
     */
    fun markFrameProcessed(frameNumber: Long) {
        _lastProcessedFrame.set(frameNumber)
    }

    /**
     * Record that a worker completed a frame
     */
    fun recordWorkerCompletion(workerId: String, frameNumber: Long) {
        // Only record if it's for the current frame
        if (frameNumber == _frameInProgress.get()) {
            currentFrameCompletions[workerId] = System.currentTimeMillis()
            log.debug("Worker {} completed logical frame {}", workerId, frameNumber)
        } else {
            log.warn("Ignoring completion from worker {} for old frame {} (current: {})",
                workerId, frameNumber, _frameInProgress.get())
        }
    }

    /**
     * Get the logical frame for a given timestamp.
     * Uses wall clock time to match operation timestamps from Kafka.
     */
    fun getLogicalFrame(timestamp: Long): Long {
        return frameCalculator.timestampToLogicalFrame(timestamp, sessionStartTimestamp)
    }

    /**
     * Get the current logical frame based on monotonic time.
     * More accurate than using wall clock time.
     */
    fun getCurrentLogicalFrame(): Long {
        return frameCalculator.getCurrentLogicalFrame(sessionStartNanos)
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
        return operationsByFrame.values.sumOf { it.size } + pendingOperations.size
    }

    /**
     * Check if frame loop is running (for monitoring)
     */
    fun isFrameLoopRunning(): Boolean {
        return _frameInProgress.get() != -1L && !isPaused
    }

    /**
     * Get current frame status (for monitoring)
     */
    fun getCurrentFrameStatus(): JsonObject {
        val currentFrame = getCurrentLogicalFrame()

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

        return frameCalculator.isApproachingBufferLimit(bufferedFrames, _frameInProgress.get())
    }
}