package com.minare.worker.coordinator

import com.hazelcast.core.HazelcastInstance
import com.minare.operation.MessageQueue
import com.minare.time.FrameConfiguration
import com.minare.time.TimeService
import com.minare.utils.VerticleLogger
import com.minare.worker.coordinator.events.*
import io.vertx.core.json.JsonArray
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
    private val messageQueue: MessageQueue,
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
     * Start a new session with announcements and manifest preparation.
     * Called on startup or after resume.
     * Operations placed into the buffer during pause will be assigned to
     * new logical frames according to the time of receipt.
     */
    private suspend fun startNewSession() {
        // Calculate session start time with frame offset for announcement
        val announcementTime = System.currentTimeMillis()
        val sessionStartTime = announcementTime + frameConfig.frameOffsetMs
        val sessionStartNanos = System.nanoTime() + (frameConfig.frameOffsetMs * 1_000_000L)

        frameManifestBuilder.clearAllManifests()
        frameCompletionTracker.clearAllCompletions()

        // Get all buffered operations
        val operationsByOldFrame = extractBufferedOperations()

        // Initialize coordinator state for new session (this clears all buffers)
        coordinatorState.startNewSession(sessionStartTime, sessionStartNanos)

        // Create logical frame manifests for buffered operations
        val newFrame = assignBufferedOperations(operationsByOldFrame)

        // Get active workers for session event
        val activeWorkers = workerRegistry.getActiveWorkers()

        // Publish session start event to Kafka for audit trail
        try {
            val sessionEvent = JsonObject()
                .put("eventType", "SESSION_START")
                .put("sessionStartTimestamp", sessionStartTime)
                .put("announcementTimestamp", announcementTime)
                .put("frameDuration", frameConfig.frameDurationMs)
                .put("frameOffset", frameConfig.frameOffsetMs)
                .put("workerCount", activeWorkers.size)
                .put("workerIds", JsonArray(activeWorkers.toList()))
                .put("coordinatorInstance", "coordinator-${System.currentTimeMillis()}") // TODO: Use a more stable ID
                .put("framesWithOperations", newFrame)

            // Send to Kafka system events topic for audit trail
            // workerCount tells us how many workers we had during this session, which protects deterministic distribution
            messageQueue.send("minare.system.events", JsonArray().add(sessionEvent)) // Wrap in JsonArray as Kafka expects batches

            log.info("Published session start event to Kafka")
        } catch (e: Exception) {
            log.error("Failed to publish session start event", e)
            // Continue anyway - this is just for audit
        }

        // Announce session start with countdown
        val announcement = JsonObject()
            .put("sessionStartTimestamp", sessionStartTime)
            .put("announcementTimestamp", announcementTime)
            .put("firstFrameStartsIn", frameConfig.frameOffsetMs)
            .put("frameDuration", frameConfig.frameDurationMs)

        vertx.eventBus().publish(ADDRESS_SESSION_START, announcement)
        log.info("Announced new session starting at {} (in {}ms) with {} workers",
            sessionStartTime, frameConfig.frameOffsetMs, activeWorkers.size)

        // Wait for the announced start time
        delay(frameConfig.frameOffsetMs)

        // Start frame scheduling from the next unprepared frame
        val nextFrameToSchedule = coordinatorState.lastPreparedManifest + 1
        scheduleFramePreparation(nextFrameToSchedule)
    }

    /**
     * Get all the operations we buffered during pause
     */
    private suspend fun extractBufferedOperations(): MutableMap<Long, List<JsonObject>> {
        // Get current buffered frame numbers before resetting state
        val oldFrameNumbers = coordinatorState.getBufferedOperationCounts().keys.sorted()

        // Extract all buffered operations while preserving their groupings
        val operationsByOldFrame = mutableMapOf<Long, List<JsonObject>>()
        oldFrameNumbers.forEach { oldFrame ->
            operationsByOldFrame[oldFrame] = coordinatorState.extractFrameOperations(oldFrame)
        }

        return operationsByOldFrame
    }

    /**
     * Assign buffered operations to logical frames by time
     */
    private suspend fun assignBufferedOperations(operationsByOldFrame: MutableMap<Long, List<JsonObject>>): Long {
        // Re-buffer operations with new frame numbers, preserving groupings
        var newFrame = 0L
        operationsByOldFrame.forEach { (_, operations) ->
            operations.forEach { op ->
                coordinatorState.bufferOperation(op, newFrame)
            }
            if (operations.isNotEmpty()) {
                log.debug("Renumbered {} operations from old frame to new frame {}",
                    operations.size, newFrame)
            }
            newFrame++
        }

        // Handle any pending operations (those that arrived before session started)
        val pendingCount = coordinatorState.getPendingOperationCount()
        if (pendingCount > 0) {
            coordinatorState.assignPendingOperationsToFrame(newFrame)
            log.info("Assigned {} pending operations to frame {}", pendingCount, newFrame)
            newFrame++
        }

        // Prepare manifests for all frames we have operations for
        val framesToPrepare = minOf(newFrame, frameConfig.maxBufferFrames.toLong())
        for (frame in 0 until framesToPrepare) {
            prepareManifestForFrame(frame)
        }

        // If no operations at all, prepare at least frames 0 and 1
        if (newFrame == 0L) {
            prepareManifestForFrame(0)
            prepareManifestForFrame(1)
        }

        log.info("Session start: renumbered {} old frames to frames 0-{}, prepared {} manifests",
            operationsByOldFrame.size, newFrame - 1, framesToPrepare)

        return newFrame
    }

    /**
     * Schedule preparation of a specific frame at its start time.
     * Ensures empty frames get manifests so workers can maintain
     * sequential frame processing and completion reporting.
     */
    private suspend fun preparePendingManifests() {
        val lastPrepared = coordinatorState.lastPreparedManifest

        if (coordinatorState.isPaused) {
            // During pause: prepare frames up to buffer limit
            val frameInProgress = coordinatorState.frameInProgress
            val maxAllowed = frameInProgress + frameConfig.maxBufferFrames

            // Find highest frame with buffered operations (don't prepare empty future frames)
            val bufferedFrames = coordinatorState.getBufferedOperationCounts().keys
            val highestBuffered = bufferedFrames.maxOrNull() ?: lastPrepared

            // Prepare up to the lesser of: highest buffered frame or max allowed
            val targetFrame = minOf(highestBuffered, maxAllowed)

            if (targetFrame > lastPrepared) {
                log.debug("During pause: preparing frames {}-{} (buffer limit: {})",
                    lastPrepared + 1, targetFrame, maxAllowed)

                for (frame in (lastPrepared + 1)..targetFrame) {
                    prepareManifestForFrame(frame)
                }
            }
        } else {
            // Normal operation: stay ahead by normalOperationLookahead
            val elapsedNanos = System.nanoTime() - coordinatorState.sessionStartNanos
            val currentWallClockFrame = elapsedNanos / (frameConfig.frameDurationMs * 1_000_000L)
            val targetFrame = currentWallClockFrame + frameConfig.normalOperationLookahead

            if (targetFrame > lastPrepared) {
                log.debug("Normal operation: preparing frames {}-{} (current wall clock: {})",
                    lastPrepared + 1, targetFrame, currentWallClockFrame)

                for (frame in (lastPrepared + 1)..targetFrame) {
                    prepareManifestForFrame(frame)
                }
            }
        }
    }

    /**
     * Schedule preparation of a specific frame at its start time.
     * Ensures empty frames get manifests for worker heartbeats.
     *
     * This is a backup mechanism - primary manifest preparation is event-driven
     * via operation arrival. This ensures frames without operations still get
     * manifests for worker heartbeats and frame progression.
     */
    private fun scheduleFramePreparation(frameNumber: Long) {
        // Don't schedule if this frame is already prepared
        if (frameNumber <= coordinatorState.lastPreparedManifest) {
            // Find the next unprepared frame
            val nextUnprepared = coordinatorState.lastPreparedManifest + 1
            scheduleFramePreparation(nextUnprepared)
            return
        }

        val frameStartNanos = coordinatorState.sessionStartNanos +
                (frameNumber * frameConfig.frameDurationMs * 1_000_000L)
        val delayMs = (frameStartNanos - System.nanoTime()) / 1_000_000L

        if (delayMs > 0) {
            // Schedule preparation at the exact frame start time
            vertx.setTimer(delayMs) {
                launch {
                    // Only prepare if still not done (operation arrival might have triggered it)
                    if (frameNumber > coordinatorState.lastPreparedManifest) {
                        if (coordinatorState.isPaused) {
                            // During pause, only prepare this frame if within buffer limits
                            val frameInProgress = coordinatorState.frameInProgress
                            val maxAllowed = frameInProgress + frameConfig.maxBufferFrames

                            if (frameNumber <= maxAllowed) {
                                prepareManifestForFrame(frameNumber)
                                // Continue scheduling during pause to ensure empty frames are handled
                                scheduleFramePreparation(frameNumber + 1)
                            } else {
                                log.debug("Not scheduling frame {} during pause - exceeds buffer limit", frameNumber)
                            }
                        } else {
                            // Normal operation - prepare frame and schedule next
                            prepareManifestForFrame(frameNumber)
                            scheduleFramePreparation(frameNumber + 1)
                        }
                    }
                }
            }
        } else {
            // We're behind schedule
            launch {
                if (coordinatorState.isPaused) {
                    // During pause, catch up but respect buffer limits
                    val frameInProgress = coordinatorState.frameInProgress
                    val maxAllowed = frameInProgress + frameConfig.maxBufferFrames
                    val currentFrame = (System.nanoTime() - coordinatorState.sessionStartNanos) /
                            (frameConfig.frameDurationMs * 1_000_000L)
                    val targetFrame = minOf(currentFrame, maxAllowed)

                    // Prepare all frames up to target
                    val lastPrepared = coordinatorState.lastPreparedManifest
                    for (frame in (lastPrepared + 1)..targetFrame) {
                        prepareManifestForFrame(frame)
                    }

                    // Schedule next frame within limits
                    if (targetFrame < maxAllowed) {
                        scheduleFramePreparation(targetFrame + 1)
                    }
                } else {
                    // Normal operation - catch up to current time
                    preparePendingManifests()

                    // Find next future frame to schedule
                    val currentFrame = (System.nanoTime() - coordinatorState.sessionStartNanos) /
                            (frameConfig.frameDurationMs * 1_000_000L)
                    scheduleFramePreparation(currentFrame + 1)
                }
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