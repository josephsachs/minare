package com.minare.core.frames.coordinator

import DistributedEnum
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.cp.IAtomicLong
import com.minare.core.frames.coordinator.services.FrameCalculatorService
import com.minare.core.frames.services.WorkerRegistry
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Shared state for the Frame Coordinator system.
 * Updated for event-driven coordination with monotonic time tracking.
 */
@Singleton
class FrameCoordinatorState @Inject constructor(
    private val workerRegistry: WorkerRegistry,
    private val frameCalculator: FrameCalculatorService,
    private val enumFactory: DistributedEnum.Factory,
    private val hazelcastInstance: HazelcastInstance
) {
    private val log = LoggerFactory.getLogger(FrameCoordinatorState::class.java)
    private val debugTraceLogs: Boolean = false

    var sessionId: String = ""

    @Volatile
    var sessionStartTimestamp: Long = 0L
        private set

    @Volatile
    var sessionStartNanos: Long = 0L
        private set

    private val _lastPreparedManifest = AtomicLong(-1L)

    var lastPreparedManifest: Long
        get() = _lastPreparedManifest.get()
        set(value) = _lastPreparedManifest.set(value)

    private val operationsByFrame = ConcurrentHashMap<Long, ConcurrentLinkedQueue<JsonObject>>()
    private val currentFrameCompletions = ConcurrentHashMap<String, Long>()
    private val snapshotEntityPartitions = ConcurrentHashMap<String, List<String>>()

    private val frameProgress: IAtomicLong = hazelcastInstance.getCPSubsystem().getAtomicLong("frame-progress")

    val frameInProgress: Long
        get() = frameProgress.get()

    private val timelineHead = AtomicLong(-1L)

    // Startup in SOFT pause until session
    private val _pauseState = enumFactory.create("pause-state", PauseState::class, PauseState.SOFT)
    private val _timelineState = enumFactory.create("timeline-state", TimelineState::class, TimelineState.PLAY)

    var pauseState: PauseState
        get() = _pauseState.get()
        set(value) {
            log.info("Pause state transitioned from {} to {}", _pauseState.get(), value)
            _pauseState.set(value)
        }

    var timelineState: TimelineState
        get() = _timelineState.get()
        set(value) {
            log.info("Timeline state changed from {} to {}", _timelineState.get(), value)
            _timelineState.set(value)
        }

    companion object {
        enum class PauseState {
            UNPAUSED,
            REST,
            SOFT,
            HARD
        }

        enum class TimelineState {
            DETACH,
            REPLAY,
            PLAY
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

        return expected.isNotEmpty() && completed.containsAll(expected)  // Added empty check
    }

    /**
     * Start a new session with specific timestamps.
     * Resets frame tracking to begin from frame -1.
     */
    fun resetSessionState(sessionStartTimestamp: Long, sessionStartNanos: Long) {
        this.sessionStartTimestamp = sessionStartTimestamp
        this.sessionStartNanos = sessionStartNanos
        _lastPreparedManifest.set(-1L)
        frameProgress.set(-1L)
        currentFrameCompletions.clear()

        if (debugTraceLogs) {
            log.info("Started new session at timestamp {} (nanos: {})",
                sessionStartTimestamp, sessionStartNanos)
        }
    }

    /**
     * Buffer an operation to a specific logical frame
     */
    fun bufferOperation(operation: JsonObject, logicalFrame: Long) {
        val queue = operationsByFrame.computeIfAbsent(logicalFrame) { ConcurrentLinkedQueue() }
        queue.offer(operation)
    }

    /**
     * Extract all operations for a specific frame.
     * Removes and returns the operations.
     */
    fun extractFrameOperations(logicalFrame: Long): List<JsonObject> {
        // Get regular buffered operations
        val queue = operationsByFrame.remove(logicalFrame) ?: return emptyList()
        return queue.toList()
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

    fun getTimelineHead(): Long {
        return timelineHead.get()
    }

    /**
     * Set timeline head position
     */
    fun setTimelineHead(frameNumber: Long) {
        timelineHead.set(frameNumber)
    }

    /**
     * Record that a worker completed a frame
     */
    fun recordWorkerCompletion(workerId: String, frameNumber: Long) {
        // Only record if it's for the current frame
        // TODO: This case should be prevented by frame completion logic/coordinator message
        if (frameNumber == frameProgress.get()) {
            currentFrameCompletions[workerId] = System.currentTimeMillis()
            if (debugTraceLogs) {
                log.debug("Worker {} completed logical frame {}", workerId, frameNumber)
            }
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
     * Get current frame status (for monitoring)
     */
    fun getCurrentFrameStatus(): JsonObject {
        val currentFrame = getCurrentLogicalFrame()

        return JsonObject()
            .put("frameInProgress", frameProgress.get())
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
     */
    fun getAllBufferedOperations(): Map<Long, List<JsonObject>> {
        return operationsByFrame.mapValues { (_, queue) -> queue.toList() }
    }

    /**
     * Clear all buffered operations (used during session reset)
     */
    fun clearAllBufferedOperations() {
        operationsByFrame.clear()
    }

    /**
     * Store a map of workerId and entityIds
     */
    fun assignEntityPartitions(partitions: Map<String, List<String>>) {
        partitions.forEach { (workerId, entityIds) ->
            snapshotEntityPartitions.set(workerId, entityIds)
        }
    }

    /**
     *
     */
    fun getEntityPartition(workerId: String): List<String> {
        return snapshotEntityPartitions.get(workerId) ?: emptyList()
    }

    /**
     * Clear all entity partition assignments
     */
    fun clearEntityPartitions() {
        snapshotEntityPartitions.clear()
    }
}