package com.minare.worker.coordinator

import com.minare.operation.MessageQueue
import com.minare.time.FrameCalculator
import com.minare.time.FrameConfiguration
import com.minare.time.TimeService
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import com.minare.worker.coordinator.events.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
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
    private val frameCalculator: FrameCalculator,
    private val vlog: VerticleLogger,
    private val eventBusUtils: EventBusUtils,
    private val workerRegistry: WorkerRegistry,
    private val coordinatorState: FrameCoordinatorState,
    private val backpressureManager: BackpressureManager,
    private val messageQueue: MessageQueue,
    private val messageQueueOperationConsumer: MessageQueueOperationConsumer,
    private val frameManifestBuilder: FrameManifestBuilder,
    private val frameCompletionTracker: FrameCompletionTracker,
    private val frameScheduler: FrameScheduler,  // NEW
    private val infraAddWorkerEvent: InfraAddWorkerEvent,
    private val infraRemoveWorkerEvent: InfraRemoveWorkerEvent,
    private val workerFrameCompleteEvent: WorkerFrameCompleteEvent,
    private val workerHeartbeatEvent: WorkerHeartbeatEvent,
    private val workerRegisterEvent: WorkerRegisterEvent,
    private val workerReadinessEvent: WorkerReadinessEvent,
    private val frameCatchUpEvent: FrameCatchUpEvent,
    private val workerHealthChangeEvent: WorkerHealthChangeEvent,
    private val nextFrameEvent: NextFrameEvent
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameCoordinatorVerticle::class.java)

    companion object {
        const val ADDRESS_FRAME_START = "minare.coordinator.frame.start"
        const val ADDRESS_FRAME_PAUSE = "minare.coordinator.frame.pause"
        const val ADDRESS_FRAME_RESUME = "minare.coordinator.frame.resume"
        const val ADDRESS_SESSION_START = "minare.coordinator.session.start"
        const val ADDRESS_FRAME_ALL_COMPLETE = "minare.coordinator.internal.frame-all-complete"
        const val ADDRESS_PREPARE_MANIFEST = "minare.coordinator.internal.prepare-manifest"
        const val ADDRESS_ALL_WORKERS_READY = "minare.coordinator.all.workers.ready"
        const val ADDRESS_WORKERS_CAUGHT_UP = "minare.coordinator.workers.caught.up"
        const val ADDRESS_NEXT_FRAME = "minare.coordinator.next.frame"  // NEW
    }

    override suspend fun start() {
        log.info("Starting FrameCoordinatorVerticle")
        vlog.setVerticle(this)

        setupEventBusConsumers()
        setupOperationConsumer()

        tryStartSession()
    }

    private fun setupEventBusConsumers() {
        // Infrastructure commands
        infraAddWorkerEvent.register()
        infraRemoveWorkerEvent.register()

        // Worker lifecycle
        launch {
            //frameCatchUpEvent.register()
            workerHeartbeatEvent.register()
            workerFrameCompleteEvent.register()
            workerRegisterEvent.register()
            workerReadinessEvent.register()
            workerHealthChangeEvent.register()
        }

        // Internal event for when all workers complete a frame
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_FRAME_ALL_COMPLETE) { msg, traceId ->
            launch {
                val logicalFrame = msg.body().getLong("logicalFrame")
                onFrameComplete(logicalFrame)
            }
        }

        // Manifest preparation requests (triggered by operation arrival)
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_PREPARE_MANIFEST) { _, traceId ->
            launch { preparePendingManifests() }
        }

        // Resume command
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_FRAME_RESUME) { msg, traceId ->
            launch {
                val reason = msg.body().getString("reason", "Manual resume")
                log.info("Received resume command: {}", reason)
                // Resume happens via FrameCatchUpEvent when workers are ready
                // Just log that we've received the request
            }
        }

        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKERS_CAUGHT_UP) { msg, traceId ->
            launch {
                val resumeFrame = msg.body().getLong("resumeFrame")
                log.info("All workers caught up, resuming at frame {}", resumeFrame)

                coordinatorState.isPaused = false
                coordinatorState.setFrameInProgress(resumeFrame)

                if (backpressureManager.isActive()) {
                    log.info("Clearing backpressure on resume from pause")
                    backpressureManager.deactivate()
                    messageQueueOperationConsumer.resumeConsumption()
                }

                // Catch up manifest preparation to current time
                preparePendingManifests()

                // Broadcast next frame event to resume processing  // NEW
                log.info("Broadcasting next frame event to resume at frame {}", resumeFrame)
                vertx.eventBus().publish(ADDRESS_NEXT_FRAME, JsonObject())

                frameScheduler.scheduleFramePreparation(resumeFrame, this) { frame ->
                    prepareManifestForFrame(frame)
                }
            }
        }

        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_ALL_WORKERS_READY) { msg, traceId ->
            launch {
                log.info("All workers ready")
                coordinatorState.isPaused = false

                // Only start session on startup
                if (coordinatorState.sessionStartTimestamp == 0L) { // TODO: Come up with a clearer way to identify startup state
                    log.info("No existing session, starting new session")
                    startNewSession()
                }
            }
        }
    }

    private suspend fun setupOperationConsumer() {
        messageQueueOperationConsumer.start(this)
    }

    /**
     * If all registered workers have already posted, unpause and start session
     */
    private suspend fun tryStartSession() {
        val existingWorkers = workerRegistry.getActiveWorkers()
        if (existingWorkers.isNotEmpty()) {
            log.info("Found {} active workers already registered", existingWorkers.size)

            if (existingWorkers.size == workerRegistry.getExpectedWorkerCount()) {
                log.info("All expected workers already active, starting session")
                coordinatorState.isPaused = false
                launch {
                    startNewSession()
                }
            }
        }
    }

    /**
     * Start a new session with announcements and manifest preparation.
     * Called on startup or after resume.
     * Operations placed into the buffer during pause will be assigned to
     * new logical frames according to the time of receipt.
     */
    private suspend fun startNewSession() {
        val announcementTime = System.currentTimeMillis()
        val sessionStartTime = announcementTime + frameConfig.frameOffsetMs
        val sessionStartNanos = System.nanoTime() + frameCalculator.msToNanos(frameConfig.frameOffsetMs)

        // Clear previous session state
        clearPreviousSessionState()

        // Extract and renumber buffered operations
        val operationsByOldFrame = extractBufferedOperations()
        coordinatorState.resetSessionState(sessionStartTime, sessionStartNanos)

        // Initialize frame progress in Hazelcast
        coordinatorState.initializeFrameProgress()

        // Reset frame in progress
        coordinatorState.setFrameInProgress(0)

        val newFrame = assignBufferedOperations(operationsByOldFrame)

        // Prepare additional manifests beyond what assignBufferedOperations did
        val minFramesToPrepare = 20L // Ensure at least 20 frames ready
        for (frame in (coordinatorState.lastPreparedManifest + 1) until minFramesToPrepare) {
            prepareManifestForFrame(frame)
        }

        // Publish session event and announce to workers
        publishSessionStartEvent(sessionStartTime, announcementTime)
        announceSessionToWorkers(sessionStartTime, announcementTime)

        // Start the periodic manifest preparation BEFORE waiting
        val manifestPrepTimer = vertx.setPeriodic(frameConfig.frameDurationMs) {
            launch {
                val currentFrame = frameCalculator.getCurrentLogicalFrame(coordinatorState.sessionStartNanos)
                val targetFrame = currentFrame + frameConfig.normalOperationLookahead

                // Prepare any missing manifests up to target
                for (frame in (coordinatorState.lastPreparedManifest + 1)..targetFrame) {
                    prepareManifestForFrame(frame)
                }
            }
        }

        // Wait for the announced start time
        delay(frameConfig.frameOffsetMs)

        // NOW broadcast - manifests should be ready
        log.info("Broadcasting initial next frame event for frame 0")
        vertx.eventBus().publish(ADDRESS_NEXT_FRAME, JsonObject())
    }

    private fun clearPreviousSessionState() {
        frameManifestBuilder.clearAllManifests()
        frameCompletionTracker.clearAllCompletions()
    }

    private suspend fun publishSessionStartEvent(sessionStartTime: Long, announcementTime: Long) {
        val activeWorkers = workerRegistry.getActiveWorkers()

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

            messageQueue.send("minare.system.events", JsonArray().add(sessionEvent))
            log.info("Published session start event to Kafka")
        } catch (e: Exception) {
            log.error("Failed to publish session start event", e)
            // Continue anyway - this is just for audit
        }
    }

    private fun announceSessionToWorkers(sessionStartTime: Long, announcementTime: Long) {
        val announcement = JsonObject()
            .put("sessionStartTimestamp", sessionStartTime)
            .put("announcementTimestamp", announcementTime)
            .put("firstFrameStartsIn", frameConfig.frameOffsetMs)
            .put("frameDuration", frameConfig.frameDurationMs)

        vertx.eventBus().publish(ADDRESS_SESSION_START, announcement)
        log.info("Announced new session starting at {} (in {}ms) with {} workers",
            sessionStartTime, frameConfig.frameOffsetMs, workerRegistry.getActiveWorkers().size)
    }

    /**
     * Get all the operations we buffered during pause
     */
    private suspend fun extractBufferedOperations(): MutableMap<Long, List<JsonObject>> {
        val oldFrameNumbers = coordinatorState.getBufferedOperationCounts().keys.sorted()
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

        log.info("Session start: renumbered {} old frames to frames 0-{}, prepared {} manifests",
            operationsByOldFrame.size, newFrame - 1, framesToPrepare)

        return newFrame
    }

    /**
     * Prepare any pending manifests based on current state.
     * PRESERVED: Original logic for determining which frames to prepare
     */
    private suspend fun preparePendingManifests() {
        val framesToPrepare = frameScheduler.getFramesToPrepareNow()

        for (frame in framesToPrepare) {
            prepareManifestForFrame(frame)
        }
    }

    /**
     * Prepare and write manifest for a specific logical frame.
     */
    private suspend fun prepareManifestForFrame(logicalFrame: Long) {
        val operations = coordinatorState.extractFrameOperations(logicalFrame)
        val activeWorkers = workerRegistry.getActiveWorkers().toSet()

        if (activeWorkers.isEmpty() && operations.isNotEmpty()) {
            log.warn("No active workers available for frame {} with {} operations",
                logicalFrame, operations.size)
        }

        val assignments = frameManifestBuilder.distributeOperations(operations, activeWorkers)

        frameManifestBuilder.writeManifestsToMap(
            logicalFrame,
            assignments,
            activeWorkers
        )

        coordinatorState.lastPreparedManifest = logicalFrame
        broadcastFrameReady(logicalFrame, operations.size, activeWorkers.size)
    }

    private fun broadcastFrameReady(logicalFrame: Long, operationCount: Int, workerCount: Int) {
        val frameReady = JsonObject()
            .put("logicalFrame", logicalFrame)
            .put("sessionStart", coordinatorState.sessionStartTimestamp)
            .put("frameDuration", frameConfig.frameDurationMs)
            .put("operationCount", operationCount)
            .put("workerCount", workerCount)

        vertx.eventBus().publish(ADDRESS_FRAME_START, frameReady)

        log.info("Prepared frame {} with {} operations for {} workers",
            logicalFrame, operationCount, workerCount)
    }

    /**
     * Handle frame completion event.
     * Triggered by WorkerFrameCompleteEvent when all workers finish.
     */
    private suspend fun onFrameComplete(logicalFrame: Long) {
        log.info("Logical frame {} completed successfully", logicalFrame)

        markFrameComplete(logicalFrame)

        // Broadcast next frame event to all workers  // NEW
        log.info("Broadcasting next frame event after completing frame {}", logicalFrame)
        vertx.eventBus().publish(ADDRESS_NEXT_FRAME, JsonObject())

        checkAndHandleBackpressure(logicalFrame)
        prepareUpcomingFrames()
        cleanupCompletedFrame(logicalFrame)
        checkFrameLag(logicalFrame)
    }

    private fun markFrameComplete(logicalFrame: Long) {
        coordinatorState.markFrameProcessed(logicalFrame)
        coordinatorState.setFrameInProgress(logicalFrame + 1)
    }

    private suspend fun checkAndHandleBackpressure(logicalFrame: Long) {
        if (!backpressureManager.isActive()) {
            return
        }

        val backpressureState = backpressureManager.getBackpressureState() ?: return

        val framesSinceActivation = logicalFrame - backpressureState.activatedAtFrame
        val currentBuffered = coordinatorState.getTotalBufferedOperations()

        log.debug("Backpressure check - frames since activation: {}, current buffer: {}/{}",
            framesSinceActivation, currentBuffered, backpressureState.maxBufferSize)

        // Deactivate if we've completed enough catch-up frames
        if (framesSinceActivation >= frameConfig.catchupFramesBeforeResume.toLong()) {
            log.info("Deactivating backpressure after {} catchup frames completed. Buffer: {}/{}",
                framesSinceActivation, currentBuffered, backpressureState.maxBufferSize)

            backpressureManager.deactivate()
            messageQueueOperationConsumer.resumeConsumption()

            // Broadcast backpressure deactivated event
            vertx.eventBus().publish(
                "minare.backpressure.deactivated",
                JsonObject()
                    .put("deactivatedAtFrame", logicalFrame)
                    .put("framesCompleted", framesSinceActivation)
                    .put("currentBufferSize", currentBuffered)
                    .put("maxBufferSize", backpressureState.maxBufferSize)
                    .put("timestamp", System.currentTimeMillis())
            )
        } else {
            val framesRemaining = frameConfig.catchupFramesBeforeResume.toLong() - framesSinceActivation
            log.info("Backpressure remains active - {} more frames needed for catchup",
                framesRemaining)
        }
    }

    private suspend fun prepareUpcomingFrames() {
        preparePendingManifests()
    }

    private fun cleanupCompletedFrame(logicalFrame: Long) {
        frameManifestBuilder.clearFrameManifests(logicalFrame)
        frameCompletionTracker.clearFrameCompletions(logicalFrame)
    }

    private fun checkFrameLag(logicalFrame: Long) {
        val expectedFrame = frameCalculator.getCurrentLogicalFrame(coordinatorState.sessionStartNanos)

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