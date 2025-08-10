package com.minare.worker.coordinator

import com.hazelcast.core.HazelcastInstance
import com.minare.time.FrameConfiguration
import com.minare.time.TimeService
import com.minare.utils.VerticleLogger
import com.minare.worker.coordinator.events.*
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * Frame Coordinator - The central orchestrator for frame-based processing.
 *
 * Event-driven implementation:
 * - No polling loops
 * - Manifest preparation triggered by events
 * - Timer-based scheduling only for frame boundaries
 * - Starts in paused state until workers ready
 */
class FrameCoordinatorVerticle @Inject constructor(
    private val frameConfig: FrameConfiguration,
    private val timeService: TimeService,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry,
    private val coordinatorState: FrameCoordinatorState,
    private val hazelcastInstance: HazelcastInstance,
    private val messageQueueOperationConsumer: MessageQueueOperationConsumer,
    private val frameManifestBuilder: FrameManifestBuilder,
    private val frameCompletionTracker: FrameCompletionTracker,
    private val frameRecoveryManager: FrameRecoveryManager,
    private val infraAddWorkerEvent: InfraAddWorkerEvent,
    private val infraRemoveWorkerEvent: InfraRemoveWorkerEvent,
    private val workerFrameCompleteEvent: WorkerFrameCompleteEvent,
    private val workerHeartbeatEvent: WorkerHeartbeatEvent
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameCoordinatorVerticle::class.java)

    companion object {
        const val ADDRESS_FRAME_START = "minare.coordinator.frame.start"
        const val ADDRESS_FRAME_PAUSE = "minare.coordinator.frame.pause"
        const val ADDRESS_FRAME_RESUME = "minare.coordinator.frame.resume"
        const val ADDRESS_SESSION_START = "minare.coordinator.session.start"
        const val ADDRESS_FRAME_ALL_COMPLETE = "minare.coordinator.internal.frame-all-complete"
        const val ADDRESS_PREPARE_MANIFEST = "minare.coordinator.internal.prepare-manifest"

        // Configuration
        const val FRAME_STARTUP_GRACE_PERIOD = 5000L
    }

    override suspend fun start() {
        log.info("Starting FrameCoordinatorVerticle")
        vlog.setVerticle(this)

        setupEventBusConsumers()
        setupOperationConsumer()

        // Check for readiness after grace period
        vertx.setTimer(FRAME_STARTUP_GRACE_PERIOD) {
            launch {
                log.info("Grace period complete, checking for worker readiness")
                checkAndStartWhenReady()
            }
        }
    }

    private fun setupEventBusConsumers() {
        // Infrastructure commands
        infraAddWorkerEvent.register()
        infraRemoveWorkerEvent.register()

        // Worker lifecycle
        launch {
            workerHeartbeatEvent.register()
            workerFrameCompleteEvent.register()
        }

        // Internal event for when all workers complete a frame
        vertx.eventBus().consumer<JsonObject>(ADDRESS_FRAME_ALL_COMPLETE) { msg ->
            launch {
                val logicalFrame = msg.body().getLong("logicalFrame")
                onFrameComplete(logicalFrame)
            }
        }

        // Manifest preparation requests (triggered by operation arrival)
        vertx.eventBus().consumer<JsonObject>(ADDRESS_PREPARE_MANIFEST) { msg ->
            launch {
                val requestedFrame = msg.body().getLong("frame")
                if (requestedFrame > coordinatorState.lastPreparedManifest) {
                    preparePendingManifests()
                }
            }
        }

        // Resume command
        vertx.eventBus().consumer<JsonObject>(ADDRESS_FRAME_RESUME) { msg ->
            launch {
                val reason = msg.body().getString("reason", "Manual resume")
                log.info("Received resume command: {}", reason)
                checkAndResumeWhenReady()
            }
        }
    }

    private suspend fun setupOperationConsumer() {
        messageQueueOperationConsumer.start(this)
    }

    /**
     * Check if cluster is ready and start a new session.
     * Called after startup grace period.
     */
    private suspend fun checkAndStartWhenReady() {
        // Keep checking until we have the expected number of workers
        while (true) {
            val expectedWorkers = workerRegistry.getExpectedWorkerCount()
            val activeWorkers = workerRegistry.getActiveWorkers()

            log.info("Worker status: {}/{} active", activeWorkers.size, expectedWorkers)

            if (activeWorkers.size == expectedWorkers) {
                log.info("All expected workers active, starting new session")
                coordinatorState.isPaused = false
                startNewSession()
                break
            } else if (activeWorkers.size > expectedWorkers) {
                log.error("Too many workers active! Expected {}, found {}",
                    expectedWorkers, activeWorkers.size)
                // Could pause here or alert operators
                break
            }

            delay(1000) // Check every second
        }
    }

    /**
     * Check if workers have caught up and resume processing.
     * Called after pause when resuming.
     */
    private suspend fun checkAndResumeWhenReady() {
        val lastProcessed = coordinatorState.lastProcessedFrame
        val expectedWorkers = workerRegistry.getExpectedWorkerCount()

        // Wait for workers to catch up to the last processed frame
        while (true) {
            val caughtUpWorkers = coordinatorState.getCompletedWorkers(lastProcessed)

            if (caughtUpWorkers.size == expectedWorkers) {
                log.info("All workers caught up to frame {}, resuming", lastProcessed)
                coordinatorState.isPaused = false
                coordinatorState.setFrameInProgress(lastProcessed + 1)

                // Catch up manifest preparation to current time
                preparePendingManifests()

                // Schedule next frame preparation
                scheduleFramePreparation(lastProcessed + 1)
                break
            }

            log.info("Waiting for workers to catch up: {}/{} ready",
                caughtUpWorkers.size, expectedWorkers)
            delay(1000)
        }
    }

    /**
     * Start a new logical frame session.
     * Called on startup or after resume.
     */
    private suspend fun startNewSession() {
        // Calculate session start time with frame offset for announcement
        val announcementTime = System.currentTimeMillis()
        val sessionStartTime = announcementTime + frameConfig.frameOffsetMs
        val sessionStartNanos = System.nanoTime() + (frameConfig.frameOffsetMs * 1_000_000L)

        frameManifestBuilder.clearAllManifests()
        frameCompletionTracker.clearAllCompletions()

        // Initialize coordinator state for new session
        coordinatorState.startNewSession(sessionStartTime, sessionStartNanos)

        // Announce session start with countdown
        val announcement = JsonObject()
            .put("sessionStartTimestamp", sessionStartTime)
            .put("announcementTimestamp", announcementTime)
            .put("firstFrameStartsIn", frameConfig.frameOffsetMs)
            .put("frameDuration", frameConfig.frameDurationMs)

        vertx.eventBus().publish(ADDRESS_SESSION_START, announcement)
        log.info("Announced new session starting at {} (in {}ms)",
            sessionStartTime, frameConfig.frameOffsetMs)

        // Wait for the announced start time
        delay(frameConfig.frameOffsetMs)

        // Prepare initial frames
        prepareManifestForFrame(0)
        prepareManifestForFrame(1)

        // Start frame scheduling
        scheduleFramePreparation(2)
    }

    /**
     * Prepare manifests for all pending frames up to current time.
     * Called when operations arrive or frames complete.
     */
    private suspend fun preparePendingManifests() {
        val elapsedNanos = System.nanoTime() - coordinatorState.sessionStartNanos
        val currentWallClockFrame = elapsedNanos / (frameConfig.frameDurationMs * 1_000_000L)

        // Determine target frame based on pause state
        val targetFrame = if (coordinatorState.isPaused) {
            // During pause, allow buffering up to MAX_BUFFER_FRAMES ahead
            val maxAllowed = coordinatorState.frameInProgress + frameConfig.maxBufferFrames
            minOf(currentWallClockFrame, maxAllowed)
        } else {
            // Normal operation: prepare slightly ahead
            currentWallClockFrame + frameConfig.normalOperationLookahead
        }

        // Prepare all outstanding frames
        val lastPrepared = coordinatorState.lastPreparedManifest
        for (frame in (lastPrepared + 1)..targetFrame) {
            prepareManifestForFrame(frame)
        }
    }

    /**
     * Schedule preparation of a specific frame at its start time.
     * Ensures empty frames get manifests for worker heartbeats.
     */
    private fun scheduleFramePreparation(frameNumber: Long) {
        if (coordinatorState.isPaused) {
            return // Don't schedule during pause
        }

        val frameStartNanos = coordinatorState.sessionStartNanos +
                (frameNumber * frameConfig.frameDurationMs * 1_000_000L)
        val delayMs = (frameStartNanos - System.nanoTime()) / 1_000_000L

        if (delayMs > 0) {
            vertx.setTimer(delayMs) {
                launch {
                    // Only prepare if not already done
                    if (frameNumber > coordinatorState.lastPreparedManifest) {
                        prepareManifestForFrame(frameNumber)
                    }
                    // Schedule next frame
                    scheduleFramePreparation(frameNumber + 1)
                }
            }
        } else {
            // We're behind, prepare immediately and catch up
            launch {
                preparePendingManifests()
                // Find next future frame to schedule
                val currentFrame = (System.nanoTime() - coordinatorState.sessionStartNanos) /
                        (frameConfig.frameDurationMs * 1_000_000L)
                scheduleFramePreparation(currentFrame + 1)
            }
        }
    }

    /**
     * Prepare and write manifest for a specific logical frame.
     */
    private suspend fun prepareManifestForFrame(logicalFrame: Long) {
        log.debug("Preparing manifest for logical frame {}", logicalFrame)

        // Get operations for this frame
        val operations = coordinatorState.extractFrameOperations(logicalFrame)

        // Get active workers
        val activeWorkers = workerRegistry.getActiveWorkers().toSet()

        if (activeWorkers.isEmpty() && operations.isNotEmpty()) {
            log.warn("No active workers available for frame {} with {} operations",
                logicalFrame, operations.size)
        }

        // Distribute operations among workers
        val assignments = frameManifestBuilder.distributeOperations(operations, activeWorkers)

        // Write manifests (even if empty - workers need them for heartbeat)
        frameManifestBuilder.writeManifestsToMap(
            logicalFrame,
            0L,  // frameEndTime deprecated
            assignments,
            activeWorkers
        )

        // Update last prepared manifest
        coordinatorState.lastPreparedManifest = logicalFrame

        // Broadcast that frame is ready
        val frameReady = JsonObject()
            .put("logicalFrame", logicalFrame)
            .put("sessionStart", coordinatorState.sessionStartTimestamp)
            .put("frameDuration", frameConfig.frameDurationMs)
            .put("operationCount", operations.size)
            .put("workerCount", activeWorkers.size)

        vertx.eventBus().publish(ADDRESS_FRAME_START, frameReady)

        log.info("Prepared frame {} with {} operations for {} workers",
            logicalFrame, operations.size, activeWorkers.size)
    }

    /**
     * Handle frame completion event.
     * Triggered by WorkerFrameCompleteEvent when all workers finish.
     */
    private suspend fun onFrameComplete(logicalFrame: Long) {
        log.info("Logical frame {} completed successfully", logicalFrame)

        // Mark frame as processed
        coordinatorState.markFrameProcessed(logicalFrame)

        // Advance to next frame
        coordinatorState.setFrameInProgress(logicalFrame + 1)

        // Prepare any pending manifests up to current time
        preparePendingManifests()

        // Clear distributed maps for this frame
        frameManifestBuilder.clearFrameManifests(logicalFrame)
        frameCompletionTracker.clearFrameCompletions(logicalFrame)

        // Check if we're falling behind
        val elapsedNanos = System.nanoTime() - coordinatorState.sessionStartNanos
        val expectedFrame = elapsedNanos / (frameConfig.frameDurationMs * 1_000_000L)

        if (expectedFrame - logicalFrame > 5) {
            log.warn("Frame processing falling behind - expected frame {}, just completed {}",
                expectedFrame, logicalFrame)
            // Could trigger pause here if needed
        }

        log.debug("Ready to process frame {}", logicalFrame + 1)
    }

    override suspend fun stop() {
        log.info("Stopping FrameCoordinatorVerticle")
        coordinatorState.isPaused = true
        messageQueueOperationConsumer.stop()
        super.stop()
    }
}