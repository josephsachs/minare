package com.minare.worker.coordinator

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.cp.IAtomicLong
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
    private val frameConfig: FrameConfiguration,
    private val hazelcastInstance: HazelcastInstance
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

    // Distributed frame progress in Hazelcast
    private val frameProgress: IAtomicLong = hazelcastInstance.getCPSubsystem().getAtomicLong("frame-progress")

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
        get() = frameProgress.get()

    /**
     * Initialize frame progress for a new session
     */
    fun initializeFrameProgress() {
        frameProgress.set(0L)
    }

    /**
     * Get all workers that have completed the specified frame
     */
    fun getCompletedWorkers(logicalFrame: Long): Set<String> {
        return if (logicalFrame == frameProgress.get()) {
            currentFrameCompletions.keys.toSet()
        } else {
            emptySet()
        }
    }

    /**
     * Check if all expected workers have completed the current frame
     */
    fun isFrameComplete(logicalFrame: Long): Boolean {
        if (logicalFrame != frameProgress.get()) {
            return false
        }

        val completed = currentFrameCompletions.keys
        val expected = workerRegistry.getActiveWorkers()

        return expected.isNotEmpty() && completed.containsAll(expected)  // Added empty check
    }

    /**
     * Start a new session with specific timestamps.
     * Resets frame tracking to begin from frame -1.
     */
    fun resetSessionState(sessionStartTimestamp: Long, sessionStartNanos: Long) {
        this.sessionStartTimestamp = sessionStartTimestamp
        this.sessionStartNanos = sessionStartNanos
        _lastProcessedFrame.set(-1L)
        _lastPreparedManifest.set(-1L)
        frameProgress.set(-1L)
        currentFrameCompletions.clear()

        log.info("Started new session at timestamp {} (nanos: {})",
            sessionStartTimestamp, sessionStartNanos)
    }

    /**
     * Buffer an operation before session starts
     */
    fun bufferPendingOperation(operation: JsonObject) {
        pendingOperations.offer(operation)
    }

    /**
     * Get count of pending operations
     */
    fun getPendingOperationCount(): Int = pendingOperations.size

    /**
     * Assign all pending operations to a specific frame
     */
    fun assignPendingOperationsToFrame(targetFrame: Long) {
        val queue = operationsByFrame.computeIfAbsent(targetFrame) { ConcurrentLinkedQueue() }

        while (pendingOperations.isNotEmpty()) {
            val op = pendingOperations.poll()
            if (op != null) {
                queue.offer(op)
            }
        }
    }

    /**
     * Buffer an operation to a specific logical frame
     */
    fun bufferOperation(operation: JsonObject, logicalFrame: Long) {
        val queue = operationsByFrame.computeIfAbsent(logicalFrame) { ConcurrentLinkedQueue() }
        queue.offer(operation)

        if (log.isDebugEnabled) {
            log.debug("Buffered operation {} to logical frame {} (queue size: {})",
                operation.getString("id"), logicalFrame, queue.size)
        }
    }

    /**
     * Extract all operations for a specific frame.
     * Removes and returns the operations.
     */
    fun extractFrameOperations(logicalFrame: Long): List<JsonObject> {
        log.info("DEBUG: Extracting operations for frame {}, available frames: {}",
            logicalFrame, operationsByFrame.keys.sorted())

        // Include pending operations if this is frame 0
        val pendingOps = if (logicalFrame == 0L && pendingOperations.isNotEmpty()) {
            val ops = mutableListOf<JsonObject>()
            while (pendingOperations.isNotEmpty()) {
                pendingOperations.poll()?.let { ops.add(it) }
            }
            ops
        } else emptyList()

        // Get regular buffered operations
        val queue = operationsByFrame.remove(logicalFrame)
        if (queue == null) {
            log.warn("No operations found for frame {}", logicalFrame)
            return pendingOps
        }

        return pendingOps + queue.toList()
    }

    /**
     * Set the frame currently in progress
     */
    fun setFrameInProgress(frameNumber: Long) {
        frameProgress.set(frameNumber)
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
        if (frameNumber == frameProgress.get()) {
            currentFrameCompletions[workerId] = System.currentTimeMillis()
            log.info("Worker {} completed logical frame {}", workerId, frameNumber)
        } else {
            log.warn("Ignoring completion from worker {} for old frame {} (current: {})",
                workerId, frameNumber, frameProgress.get())
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
     * Get the number of frames that have buffered operations.
     * This is used to enforce frame buffer limits.
     *
     * @return The count of frames beyond frameInProgress that have operations buffered
     */
    fun getBufferedFrameCount(): Int {
        if (frameProgress.get() < 0) {
            // Session hasn't started yet, count all frames
            return operationsByFrame.size
        }

        val currentFrame = frameProgress.get()
        val bufferedFrames = operationsByFrame.keys.filter { it >= currentFrame }

        return if (bufferedFrames.isEmpty()) {
            0
        } else {
            val maxBufferedFrame = bufferedFrames.maxOrNull() ?: currentFrame
            (maxBufferedFrame - currentFrame + 1).toInt()
        }
    }

    /**
     * Check if frame loop is running (for monitoring)
     */
    fun isFrameLoopRunning(): Boolean {
        return frameProgress.get() != -1L && !isPaused
    }

    /**
     * Get current frame status (for monitoring)
     */
    fun getCurrentFrameStatus(): JsonObject {
        val currentFrame = getCurrentLogicalFrame()

        return JsonObject()
            .put("frameInProgress", frameProgress.get())
            .put("lastProcessedFrame", _lastProcessedFrame.get())
            .put("lastPreparedManifest", _lastPreparedManifest.get())
            .put("currentWallClockFrame", currentFrame)
            .put("sessionStartTimestamp", sessionStartTimestamp)
            .put("completedWorkers", currentFrameCompletions.size)
            .put("totalWorkers", workerRegistry.getActiveWorkers().size)
            .put("isPaused", isPaused)
            .put("totalBufferedOperations", getTotalBufferedOperations())
            .put("bufferedFrameCount", getBufferedFrameCount())
    }

    /**
     * Check if we're approaching buffer limits during pause
     */
    fun isApproachingBufferLimit(): Boolean {
        if (!isPaused) return false

        val bufferedFrames = operationsByFrame.keys.maxOrNull()?.let { maxFrame ->
            maxFrame - frameProgress.get()
        } ?: 0

        return bufferedFrames > frameConfig.maxBufferFrames * 0.8
    }

    /**
     * Get all buffered operations (used during session start)
     * Preserves original groupings by frame.
     */
    fun getAllBufferedOperations(): Map<Long, List<JsonObject>> {
        return operationsByFrame.mapValues { (_, queue) -> queue.toList() }
    }

    /**
     * Clear all buffered operations (used during session reset)
     */
    fun clearAllBufferedOperations() {
        operationsByFrame.clear()
        pendingOperations.clear()
    }
}