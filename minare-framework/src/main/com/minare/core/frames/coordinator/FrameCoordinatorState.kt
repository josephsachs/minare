package com.minare.core.frames.coordinator

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.cp.IAtomicLong
import com.minare.core.frames.coordinator.services.FrameCalculatorService
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.utils.vertx.EventBusUtils
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Shared state for the Frame Coordinator system.
 * Updated for event-driven coordination with monotonic time tracking.
 *
 * DEDUPLICATION UPDATE: Changed operationsByFrame from ConcurrentHashMap<Long, ConcurrentLinkedQueue<JsonObject>>
 * to ConcurrentHashMap<Long, ConcurrentHashMap<String, JsonObject>> to automatically deduplicate operations
 * by ID within each frame.
 */
@Singleton
class FrameCoordinatorState @Inject constructor(
    private val workerRegistry: WorkerRegistry,
    private val frameCalculator: FrameCalculatorService,
    private val hazelcastInstance: HazelcastInstance,
    private val eventBusUtils: EventBusUtils
) {
    private val log = LoggerFactory.getLogger(FrameCoordinatorState::class.java)

    @Volatile
    var sessionStartTimestamp: Long = 0L
        private set

    @Volatile
    var sessionStartNanos: Long = 0L
        private set

    private val _lastProcessedFrame = AtomicLong(-1L)
    private val _lastPreparedManifest = AtomicLong(-1L)

    // CHANGED: Now maps frame number to a map of operation ID to operation for deduplication
    private val operationsByFrame = ConcurrentHashMap<Long, ConcurrentHashMap<String, JsonObject>>()
    private val pendingOperations = ConcurrentLinkedQueue<JsonObject>()
    private val currentFrameCompletions = ConcurrentHashMap<String, Long>()
    private val frameProgress: IAtomicLong = hazelcastInstance.getCPSubsystem().getAtomicLong("frame-progress")

    var sessionId: String = ""

    private var _pauseState: PauseState = PauseState.UNPAUSED

    var lastProcessedFrame: Long
        get() = _lastProcessedFrame.get()
        private set(value) = _lastProcessedFrame.set(value)

    var lastPreparedManifest: Long
        get() = _lastPreparedManifest.get()
        set(value) = _lastPreparedManifest.set(value)

    val frameInProgress: Long
        get() = frameProgress.get()

    var pauseState: PauseState
        get() = _pauseState
        set(value) {
            log.info("Pause state transitioned from {} to {}", _pauseState, value)
            _pauseState = value
        }

    companion object {
        enum class PauseState {
            UNPAUSED,
            REST,
            SOFT,
            HARD
        }
    }

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

        return expected.isNotEmpty() && completed.containsAll(expected)
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
     * NOTE: This method may need updating if pending operations should also be deduplicated
     */
    fun assignPendingOperationsToFrame(targetFrame: Long) {
        val frameOps = operationsByFrame.computeIfAbsent(targetFrame) { ConcurrentHashMap() }

        while (pendingOperations.isNotEmpty()) {
            val op = pendingOperations.poll()
            if (op != null) {
                val operationId = op.getString("id")
                if (operationId != null) {
                    frameOps.putIfAbsent(operationId, op)
                } else {
                    log.warn("Pending operation missing ID, skipping: {}", op.encode())
                }
            }
        }
    }

    /**
     * Buffer an operation to a specific logical frame.
     * UPDATED: Now uses putIfAbsent for automatic deduplication by operation ID
     */
    fun bufferOperation(operation: JsonObject, logicalFrame: Long) {
        val frameOps = operationsByFrame.computeIfAbsent(logicalFrame) { ConcurrentHashMap() }
        val operationId = operation.getString("id")
            ?: throw IllegalArgumentException("Operation missing ID: ${operation.encode()}")

        val previous = frameOps.putIfAbsent(operationId, operation)

        if (previous != null) {
            log.debug("Skipping duplicate operation {} in frame {}", operationId, logicalFrame)
        } else if (log.isDebugEnabled) {
            log.debug("Buffered operation {} to logical frame {} (frame size: {})",
                operationId, logicalFrame, frameOps.size)
        }
    }

    /**
     * Extract all operations for a specific frame.
     * Removes and returns the operations.
     * UPDATED: Now extracts from a map instead of a queue, returns operations sorted by ID
     */
    fun extractFrameOperations(logicalFrame: Long): List<JsonObject> {
        // Include pending operations if this is frame 0
        // TODO: This probably shouldn't work like this
        val pendingOps = if (logicalFrame == 0L && pendingOperations.isNotEmpty()) {
            val ops = mutableListOf<JsonObject>()
            while (pendingOperations.isNotEmpty()) {
                pendingOperations.poll()?.let { ops.add(it) }
            }
            ops
        } else emptyList()

        // Get regular buffered operations - now from a map instead of queue
        val frameOps = operationsByFrame.remove(logicalFrame)

        // TEMPORARY DEBUG
        log.info("extractFrameOperations({}) extracted {} operations",
            logicalFrame, frameOps?.size ?: 0)

        if (frameOps == null) {
            log.debug("No operations found for frame {}", logicalFrame)
            return pendingOps
        }

        // Return operations sorted by ID for deterministic ordering
        val sortedOps = frameOps.values.sortedBy { it.getString("id") }
        return pendingOps + sortedOps
    }

    /**
     * Check if frame loop is running
     */
    fun isFrameLoopRunning(): Boolean {
        return frameProgress.get() != -1L
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
            log.debug("Worker {} completed logical frame {}", workerId, frameNumber)
        } else {
            log.error("Ignoring completion from worker {} for old frame {} (current: {})",
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
     * UPDATED: Now gets size from the inner map
     */
    fun getBufferedOperationCounts(): Map<Long, Int> {
        return operationsByFrame.mapValues { it.value.size }
    }

    /**
     * Get total buffered operations across all frames
     * UPDATED: Now sums the sizes of the inner maps
     */
    fun getTotalBufferedOperations(): Int {
        return operationsByFrame.values.sumOf { it.size } + pendingOperations.size
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
            .put("totalBufferedOperations", getTotalBufferedOperations())
    }

    /**
     * Get all buffered operations (used during session start)
     * Preserves original groupings by frame.
     * UPDATED: Extracts values from the inner maps and sorts by ID
     */
    fun getAllBufferedOperations(): Map<Long, List<JsonObject>> {
        return operationsByFrame.mapValues { (_, frameOps) ->
            frameOps.values.sortedBy { it.getString("id") }
        }
    }

    /**
     * Clear all buffered operations (used during session reset)
     */
    fun clearAllBufferedOperations() {
        operationsByFrame.clear()
        pendingOperations.clear()
    }
}