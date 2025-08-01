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
 * Updated for logical frames:
 * - Continuous processing instead of scheduled execution
 * - Logical frame numbering from session start
 * - Manifest pre-computation for future frames
 * - Deterministic operation ordering
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

        // Configuration
        const val FRAME_STARTUP_GRACE_PERIOD = 5000L
        const val MAX_FUTURE_MANIFESTS = 10  // How many frames ahead to prepare
    }

    override suspend fun start() {
        log.info("Starting FrameCoordinatorVerticle")
        vlog.setVerticle(this)

        setupEventBusConsumers()
        setupOperationConsumer()

        // Start a new session after grace period
        vertx.setTimer(FRAME_STARTUP_GRACE_PERIOD) {
            launch {
                log.info("Starting new frame processing session")
                startNewSession()
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

        // Resume command
        vertx.eventBus().consumer<JsonObject>(ADDRESS_FRAME_RESUME) { msg ->
            launch {
                val reason = msg.body().getString("reason", "Manual resume")
                log.info("Received resume command: {}", reason)
                startNewSession()
            }
        }
    }

    private suspend fun setupOperationConsumer() {
        messageQueueOperationConsumer.start(this)
    }

    /**
     * Start a new logical frame session.
     * Called on startup or after resume.
     */
    private suspend fun startNewSession() {
        // Calculate session start time with frame offset for announcement
        val announcementTime = System.currentTimeMillis()
        val sessionStartTime = announcementTime + frameConfig.frameOffsetMs

        frameManifestBuilder.clearAllManifests()
        frameCompletionTracker.clearAllCompletions()

        // Initialize coordinator state for new session
        coordinatorState.startNewSession(sessionStartTime)

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

        // Start the continuous frame processing loop
        launchFrameProcessingLoop()
    }

    /**
     * Launch the continuous frame processing loop.
     * Prepares manifests ahead of time and tracks completions.
     */
    private fun launchFrameProcessingLoop() = launch {
        log.info("Starting continuous frame processing loop")

        while (!coordinatorState.isPaused) {
            try {
                // Prepare manifests for future frames
                prepareUpcomingManifests()

                // Check if we need to advance lastProcessedFrame
                checkFrameCompletions()

                // Brief pause to avoid spinning
                delay(50)  // Check every 50ms

            } catch (e: Exception) {
                log.error("Error in frame processing loop", e)
                coordinatorState.isPaused = true
                frameRecoveryManager.pauseFrameProcessing("Processing loop error: ${e.message}")
            }
        }

        log.info("Frame processing loop stopped (paused: {})", coordinatorState.isPaused)
    }

    /**
     * Prepare manifests for upcoming logical frames.
     * Stays ahead of processing by preparing multiple future frames.
     */
    private suspend fun prepareUpcomingManifests() {
        val currentTime = System.currentTimeMillis()
        val sessionStart = coordinatorState.sessionStartTimestamp

        // Calculate which logical frame we should be preparing
        val elapsedTime = currentTime - sessionStart
        val currentLogicalFrame = if (elapsedTime >= 0) {
            elapsedTime / frameConfig.frameDurationMs
        } else {
            return // Not time yet
        }

        val lastPrepared = coordinatorState.lastPreparedManifest
        val targetFrame = minOf(
            currentLogicalFrame + MAX_FUTURE_MANIFESTS,
            lastPrepared + MAX_FUTURE_MANIFESTS
        )

        // Prepare manifests for frames we haven't prepared yet
        for (frame in (lastPrepared + 1)..targetFrame) {
            prepareManifestForFrame(frame)
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
        val activeWorkers = workerRegistry.getActiveWorkers()

        if (activeWorkers.isEmpty() && operations.isNotEmpty()) {
            log.warn("No active workers available for frame {} with {} operations",
                logicalFrame, operations.size)
        }

        // Distribute operations among workers
        val assignments = frameManifestBuilder.distributeOperations(operations, activeWorkers)

        // Write manifests (even if empty)
        frameManifestBuilder.writeManifestsToMap(
            logicalFrame,  // Now using logical frame number
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
     * Check frame completions and advance lastProcessedFrame.
     */
    private suspend fun checkFrameCompletions() {
        val lastProcessed = coordinatorState.lastProcessedFrame
        val nextFrame = lastProcessed + 1

        // Check if the next frame is complete
        if (coordinatorState.isFrameComplete(nextFrame)) {
            onFrameComplete(nextFrame)
        }

        // Also check if we're falling behind
        val currentTime = System.currentTimeMillis()
        val expectedFrame = (currentTime - coordinatorState.sessionStartTimestamp) / frameConfig.frameDurationMs

        if (expectedFrame - lastProcessed > 5) {
            log.warn("Frame processing falling behind - expected frame {}, last processed {}",
                expectedFrame, lastProcessed)
            // Could trigger pause here if needed
        }
    }

    /**
     * Handle frame completion.
     */
    private suspend fun onFrameComplete(logicalFrame: Long) {
        log.info("Logical frame {} completed successfully", logicalFrame)

        // Mark frame as processed
        coordinatorState.markFrameProcessed(logicalFrame)

        // Clear distributed maps for this frame
        frameManifestBuilder.clearAllManifests()
        frameCompletionTracker.clearAllCompletions()

        // Continue with next frame
        log.debug("Ready to process frame {}", logicalFrame + 1)
    }

    /**
     * Request health check from monitor.
     * Updated to use logical frame numbers.
     */
    private fun requestHealthCheck(logicalFrame: Long): io.vertx.core.Future<JsonObject> {
        val promise = io.vertx.core.Promise.promise<JsonObject>()

        val healthCheckGracePeriod = (frameConfig.frameDurationMs * 0.2).toLong()

        // Set up one-time consumer for the response
        val consumer = vertx.eventBus().consumer<JsonObject>(
            FrameWorkerHealthMonitorVerticle.ADDRESS_HEALTH_CHECK_RESULT
        ) { message ->
            val result = message.body()
            if (result.getLong("logicalFrame") == logicalFrame) {
                promise.complete(result)
            }
        }

        // Request health check
        vertx.eventBus().send(
            FrameWorkerHealthMonitorVerticle.ADDRESS_HEALTH_CHECK_REQUEST,
            JsonObject().put("logicalFrame", logicalFrame)
        )

        // Timeout after grace period
        vertx.setTimer(healthCheckGracePeriod) {
            consumer.unregister()
            if (!promise.future().isComplete) {
                promise.fail("Health check timeout")
            }
        }

        return promise.future()
    }

    override suspend fun stop() {
        log.info("Stopping FrameCoordinatorVerticle")
        coordinatorState.isPaused = true
        messageQueueOperationConsumer.stop()
        super.stop()
    }
}