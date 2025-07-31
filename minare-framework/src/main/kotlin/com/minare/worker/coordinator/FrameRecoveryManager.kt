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
 * This component handles the coordination of recovery efforts when
 * workers fail or frames fall behind.
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
    }

    /**
     * Pause frame processing with a reason.
     * Broadcasts pause event to all components.
     *
     * @param reason The reason for pausing
     */
    suspend fun pauseFrameProcessing(reason: String) {
        log.warn("Pausing frame processing: {}", reason)
        coordinatorState.isPaused = true

        val pauseEvent = JsonObject()
            .put("reason", reason)
            .put("timestamp", System.currentTimeMillis())
            .put("currentFrame", coordinatorState.currentFrameStart)

        vertx.eventBus().publish(FrameCoordinatorVerticle.ADDRESS_FRAME_PAUSE, pauseEvent)
    }

    /**
     * Resume frame processing after a pause.
     * Broadcasts resume event to all components.
     *
     * @param reason The reason for resuming (e.g., "Recovery complete")
     */
    suspend fun resumeFrameProcessing(reason: String = "Manual resume") {
        log.info("Resuming frame processing: {}", reason)
        coordinatorState.isPaused = false

        val resumeEvent = JsonObject()
            .put("reason", reason)
            .put("timestamp", System.currentTimeMillis())
            .put("resumeFrame", coordinatorState.currentFrameStart)

        vertx.eventBus().publish(FrameCoordinatorVerticle.ADDRESS_FRAME_RESUME, resumeEvent)
    }

    /**
     * Attempt to recover from missing workers for a specific frame.
     *
     * @param frameStartTime The frame to recover
     * @param missingWorkers Set of worker IDs that are missing
     * @return RecoveryResult indicating success/failure and next steps
     */
    suspend fun attemptFrameRecovery(
        frameStartTime: Long,
        missingWorkers: Set<String>
    ): RecoveryResult {
        log.info("Attempting recovery for frame {} with missing workers: {}",
            frameStartTime, missingWorkers)

        // Give workers a chance to recover
        delay(RECOVERY_TIMEOUT)

        // Check if missing workers are back
        val stillMissing = missingWorkers.filter { workerId ->
            val state = workerRegistry.getWorkerState(workerId)
            state == null || state.status != WorkerRegistry.WorkerStatus.ACTIVE
        }

        return if (stillMissing.isEmpty()) {
            log.info("All workers recovered for frame {}", frameStartTime)
            RecoveryResult(
                success = true,
                recoveredWorkers = missingWorkers,
                remainingMissing = emptySet(),
                recommendation = RecoveryRecommendation.CONTINUE
            )
        } else {
            log.warn("Recovery failed - {} workers still missing for frame {}",
                stillMissing.size, frameStartTime)

            RecoveryResult(
                success = false,
                recoveredWorkers = missingWorkers - stillMissing.toSet(),
                remainingMissing = stillMissing.toSet(),
                recommendation = determineRecoveryRecommendation(stillMissing.size)
            )
        }
    }

    /**
     * Determine the recommended action based on the number of missing workers.
     */
    private fun determineRecoveryRecommendation(missingCount: Int): RecoveryRecommendation {
        val totalWorkers = workerRegistry.getActiveWorkers().size + missingCount
        val missingPercentage = (missingCount.toDouble() / totalWorkers) * 100

        return when {
            missingPercentage > 50 -> RecoveryRecommendation.PAUSE_AND_WAIT
            missingPercentage > 25 -> RecoveryRecommendation.CONTINUE_DEGRADED
            else -> RecoveryRecommendation.CONTINUE
        }
    }
}

/**
 * Result of a recovery attempt.
 */
data class RecoveryResult(
    val success: Boolean,
    val recoveredWorkers: Set<String>,
    val remainingMissing: Set<String>,
    val recommendation: RecoveryRecommendation
)

/**
 * Recommendations for how to proceed after recovery attempt.
 */
enum class RecoveryRecommendation {
    CONTINUE,           // All good, continue normally
    CONTINUE_DEGRADED,  // Continue but with reduced capacity
    PAUSE_AND_WAIT,     // Too many failures, pause and wait for manual intervention
    RETRY_RECOVERY      // Try recovery again
}