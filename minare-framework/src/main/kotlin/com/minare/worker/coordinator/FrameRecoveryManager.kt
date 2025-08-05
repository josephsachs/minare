package com.minare.worker.coordinator

import com.minare.time.FrameConfiguration
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.delay
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Manages frame recovery operations including pausing, recovery attempts,
 * and resuming frame processing.
 *
 * Updated for event-driven coordination:
 * - Pause no longer stops manifest preparation
 * - Resume checks for worker readiness
 * - Recovery decisions based on completion events
 */
@Singleton
class FrameRecoveryManager @Inject constructor(
    private val vertx: Vertx,
    private val frameConfig: FrameConfiguration,
    private val coordinatorState: FrameCoordinatorState,
    private val workerRegistry: WorkerRegistry,
    private val frameCompletionTracker: FrameCompletionTracker
) {
    private val log = LoggerFactory.getLogger(FrameRecoveryManager::class.java)

    companion object {
        const val RECOVERY_TIMEOUT = 5000L
        const val HEALTH_CHECK_INTERVAL = 2000L
    }

    /**
     * Pause frame processing with a reason.
     * Broadcasts pause event to all components.
     *
     * Note: During pause, operations continue to be buffered and
     * manifests continue to be prepared (up to buffer limit).
     *
     * @param reason The reason for pausing
     */
    suspend fun pauseFrameProcessing(reason: String) {
        log.warn("Pausing frame processing: {}", reason)
        coordinatorState.isPaused = true

        val currentFrame = coordinatorState.frameInProgress
        val bufferedOps = coordinatorState.getTotalBufferedOperations()

        val pauseEvent = JsonObject()
            .put("reason", reason)
            .put("timestamp", System.currentTimeMillis())
            .put("pausedAtFrame", currentFrame)
            .put("bufferedOperations", bufferedOps)

        vertx.eventBus().publish(FrameCoordinatorVerticle.ADDRESS_FRAME_PAUSE, pauseEvent)

        log.info("Frame processing paused at frame {} with {} buffered operations",
            currentFrame, bufferedOps)
    }

    /**
     * Request resume of frame processing.
     * Actual resume happens when workers are ready.
     *
     * @param reason The reason for resuming (e.g., "Recovery complete")
     */
    suspend fun requestFrameResume(reason: String = "Manual resume") {
        log.info("Resume requested: {}", reason)

        val resumeEvent = JsonObject()
            .put("reason", reason)
            .put("timestamp", System.currentTimeMillis())
            .put("requestedAtFrame", coordinatorState.frameInProgress)

        // Send resume request - coordinator will check readiness
        vertx.eventBus().publish(FrameCoordinatorVerticle.ADDRESS_FRAME_RESUME, resumeEvent)
    }

    /**
     * Check if recovery is needed based on frame completion status.
     * Called when frames are falling behind or workers are missing.
     *
     * @param expectedFrame The frame we should be at based on wall clock
     * @param actualFrame The frame we're actually processing
     * @return true if recovery action was taken
     */
    suspend fun checkRecoveryNeeded(expectedFrame: Long, actualFrame: Long): Boolean {
        val frameLag = expectedFrame - actualFrame

        if (frameLag <= 2) {
            // Minor lag is acceptable
            return false
        }

        log.warn("Frame processing lagging by {} frames (expected: {}, actual: {})",
            frameLag, expectedFrame, actualFrame)

        // Check worker health
        val activeWorkers = workerRegistry.getActiveWorkers()
        val expectedWorkers = workerRegistry.getExpectedWorkerCount()

        if (activeWorkers.size < expectedWorkers) {
            log.error("Missing workers: {}/{} active", activeWorkers.size, expectedWorkers)
            pauseFrameProcessing("Insufficient workers: ${activeWorkers.size}/${expectedWorkers}")
            return true
        }

        // Check for stuck workers
        val stuckWorkers = findStuckWorkers(actualFrame)
        if (stuckWorkers.isNotEmpty()) {
            log.error("Workers stuck on frame {}: {}", actualFrame, stuckWorkers)
            pauseFrameProcessing("Workers stuck: ${stuckWorkers.joinToString()}")
            return true
        }

        // If we're severely behind but workers seem OK, might need to skip frames
        if (frameLag > 10) {
            log.error("Severe frame lag ({} frames), considering frame skip", frameLag)
            // Could implement frame skipping logic here
            pauseFrameProcessing("Severe frame lag: $frameLag frames behind")
            return true
        }

        return false
    }

    /**
     * Find workers that haven't completed the current frame.
     */
    private fun findStuckWorkers(frameNumber: Long): List<String> {
        val missingWorkers = frameCompletionTracker.getMissingWorkers(frameNumber)
        val activeWorkers = workerRegistry.getActiveWorkers()

        // Workers that are active but haven't completed the frame
        return missingWorkers.intersect(activeWorkers.toSet()).toList()
    }

    /**
     * Monitor worker health during pause.
     * Can be called periodically to check if conditions have improved.
     */
    suspend fun monitorRecoveryProgress() {
        if (!coordinatorState.isPaused) {
            return
        }

        val pausedFrame = coordinatorState.frameInProgress
        val activeWorkers = workerRegistry.getActiveWorkers()
        val expectedWorkers = workerRegistry.getExpectedWorkerCount()
        val bufferedOps = coordinatorState.getTotalBufferedOperations()
        val bufferFull = coordinatorState.isApproachingBufferLimit()

        log.info("Recovery status - Frame: {}, Workers: {}/{}, Buffered ops: {}, Buffer full: {}",
            pausedFrame, activeWorkers.size, expectedWorkers, bufferedOps, bufferFull)

        // Check if we should attempt resume
        if (activeWorkers.size == expectedWorkers) {
            // Check if workers have caught up
            val completedWorkers = frameCompletionTracker.getCompletedWorkers(pausedFrame)
            if (completedWorkers.size == expectedWorkers) {
                log.info("All workers caught up, requesting resume")
                requestFrameResume("All workers recovered and caught up")
            }
        }

        // Warn if buffer is getting full
        if (bufferFull) {
            log.warn("Operation buffer approaching limit during pause - backpressure needed")
        }
    }

    /**
     * Handle worker failure during frame processing.
     *
     * @param workerId The failed worker
     * @param frameNumber The frame being processed
     */
    suspend fun handleWorkerFailure(workerId: String, frameNumber: Long) {
        log.error("Worker {} failed during frame {}", workerId, frameNumber)

        val activeWorkers = workerRegistry.getActiveWorkers()
        val remainingWorkers = activeWorkers.filter { it != workerId }

        if (remainingWorkers.isEmpty()) {
            log.error("No workers remaining after {} failed", workerId)
            pauseFrameProcessing("All workers failed")
        } else if (remainingWorkers.size < (workerRegistry.getExpectedWorkerCount() / 2)) {
            log.error("Too few workers remaining ({}) after {} failed",
                remainingWorkers.size, workerId)
            pauseFrameProcessing("Insufficient workers after failure: $workerId")
        } else {
            log.warn("Continuing with {} workers after {} failed",
                remainingWorkers.size, workerId)
            // Could redistribute work here
        }
    }

    /**
     * Clean up recovery state (called after successful resume).
     */
    fun clearRecoveryState() {
        log.debug("Clearing recovery state after successful resume")
        // Any recovery-specific state cleanup would go here
    }

    /**
     * Get current recovery status for monitoring.
     */
    fun getRecoveryStatus(): JsonObject {
        val status = JsonObject()
            .put("isPaused", coordinatorState.isPaused)
            .put("pausedAtFrame", coordinatorState.frameInProgress)
            .put("activeWorkers", workerRegistry.getActiveWorkers().size)
            .put("expectedWorkers", workerRegistry.getExpectedWorkerCount())
            .put("bufferedOperations", coordinatorState.getTotalBufferedOperations())
            .put("bufferApproachingLimit", coordinatorState.isApproachingBufferLimit())

        if (coordinatorState.isPaused) {
            val missingWorkers = frameCompletionTracker.getMissingWorkers(
                coordinatorState.frameInProgress
            )
            status.put("workersNotCaughtUp", missingWorkers.size)
        }

        return status
    }
}