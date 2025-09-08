package com.minare.core.frames.coordinator

import com.minare.application.config.FrameConfiguration
import com.minare.core.frames.coordinator.FrameCoordinatorState.Companion.PauseState
import com.minare.core.frames.coordinator.handlers.LateOperationDecision
import com.minare.core.frames.coordinator.handlers.LateOperationHandler
import com.minare.core.frames.coordinator.services.*
import com.minare.core.frames.coordinator.services.SessionService.Companion.ADDRESS_SESSION_INITIALIZED
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.operation.interfaces.MessageQueue
import com.minare.core.utils.debug.OperationDebugUtils
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.EventWaiter
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.worker.coordinator.events.*
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import javax.inject.Inject
import kotlin.math.max

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
    private val frameCalculator: FrameCalculatorService,
    private val lateOperationHandler: LateOperationHandler,
    private val vlog: VerticleLogger,
    private val eventBusUtils: EventBusUtils,
    private val eventWaiter: EventWaiter,
    private val workerRegistry: WorkerRegistry,
    private val coordinatorState: FrameCoordinatorState,
    private val messageQueueOperationConsumer: MessageQueueOperationConsumer,
    private val frameManifestBuilder: FrameManifestBuilder,
    private val frameCompletionTracker: FrameCompletionTracker,
    private val startupService: StartupService,
    private val sessionService: SessionService,
    private val infraAddWorkerEvent: InfraAddWorkerEvent,
    private val infraRemoveWorkerEvent: InfraRemoveWorkerEvent,
    private val workerFrameCompleteEvent: WorkerFrameCompleteEvent,
    private val workerHeartbeatEvent: WorkerHeartbeatEvent,
    private val workerRegisterEvent: WorkerRegisterEvent,
    private val workerReadinessEvent: WorkerReadinessEvent,
    private val workerHealthChangeEvent: WorkerHealthChangeEvent,
    private val operationDebugUtils: OperationDebugUtils
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameCoordinatorVerticle::class.java)

    var manifestTimerId: Long? = null

    companion object {
        const val ADDRESS_FRAME_START = "minare.coordinator.frame.start"
        const val ADDRESS_SESSION_START = "minare.coordinator.session.start"
        const val ADDRESS_FRAME_ALL_COMPLETE = "minare.coordinator.internal.frame-all-complete"
        const val ADDRESS_FRAME_MANIFESTS_ALL_COMPLETE = "minare.coordinator.internal.frame.manifests-all-complete"
        const val ADDRESS_PREPARE_SESSION_MANIFESTS = "minare.coordinator.prepare-session-manifests"
        const val ADDRESS_SESSION_MANIFESTS_PREPARED = "minare.coordinator.session-manifests-prepared"
        const val ADDRESS_NEXT_FRAME = "minare.coordinator.next.frame"  // NEW
    }

    override suspend fun start() {
        log.info("Starting FrameCoordinatorVerticle")
        vlog.setVerticle(this)

        setupEventBusConsumers()
        setupOperationConsumer()

        startupService.checkInitialWorkerStatus()

        launch {
            log.info("Waiting for all workers to be ready...")
            startupService.awaitAllWorkersReady()
            log.info("All workers ready, starting session")
            startupSession()
        }
    }

    private fun setupEventBusConsumers() {
        infraAddWorkerEvent.register()
        infraRemoveWorkerEvent.register()

        launch {
            workerHeartbeatEvent.register()
            workerFrameCompleteEvent.register()
            workerRegisterEvent.register()
            workerReadinessEvent.register()
            workerHealthChangeEvent.register()
        }

        // All workers complete a frame
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_FRAME_ALL_COMPLETE) { msg, traceId ->
            launch {
                val logicalFrame = msg.body().getLong("logicalFrame")
                onFrameComplete(logicalFrame)
            }
        }

        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_PREPARE_SESSION_MANIFESTS) { msg, traceId ->
            launch {
                val sessionStartTime = msg.body().getLong("sessionStartTime")
                val sessionStartNanos = msg.body().getLong("sessionStartNanos")

                val operationsByOldFrame = extractBufferedOperations()

                coordinatorState.resetSessionState(sessionStartTime, sessionStartNanos)
                coordinatorState.initializeFrameProgress()
                coordinatorState.setFrameInProgress(0)

                prepareManifestForFrame(0) // 09-08-25 extractBufferedOperations doesn't do anything
                                                      // since we've already done it, but hardcoding the value to frame 0
                                                      // is damn suspicious considering the exact shape of the issue
                assignBufferedOperations(operationsByOldFrame)

                eventBusUtils.publishWithTracing(
                    ADDRESS_SESSION_MANIFESTS_PREPARED,
                    JsonObject()
                )
            }
        }
    }

    private suspend fun setupOperationConsumer() {
        messageQueueOperationConsumer.start(this)
    }

    private suspend fun startManifestTimer() {
        // IMPORTANT: Continue preparing manifests even without frame completions
        // This keeps operation processing responsive to real-time input
        manifestTimerId = vertx.setPeriodic(frameConfig.frameDurationMs) {
            launch {
                val currentFrame = frameCalculator.getCurrentLogicalFrame(coordinatorState.sessionStartNanos)
                val targetFrame = currentFrame + frameConfig.normalOperationLookahead
                for (frame in (coordinatorState.lastPreparedManifest + 1)..targetFrame) {
                    prepareManifestForFrame(frame)
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
    private suspend fun startupSession() {
        // SessionService takes over setup, returning control to
        // ADDRESS_PREPARE_SESSION_MANIFESTS before concluding
        sessionService.initializeSession()

        val eventMessage = eventWaiter.waitForEvent(ADDRESS_SESSION_INITIALIZED)

        val newSessionId = eventMessage.getString("sessionId")

        coordinatorState.sessionId = newSessionId

        startManifestTimer()
        sessionService.startSession()
    }

    /**
     * Assign buffered operations to logical frames by time
     */
    private suspend fun assignBufferedOperations(operationsByOldFrame: MutableMap<Long, List<JsonObject>>): Long {
        // Re-buffer operations with new frame numbers, preserving groupings
        // Not sure we should be doing this here
        var newFrame = 0L

        operationsByOldFrame.forEach { (_, operations) ->
            operations.forEach { op ->
                val timestamp = op.getLong("timestamp")
                val calculatedFrame = coordinatorState.getLogicalFrame(timestamp)

                // TEMPORARY DEBUG
                operationDebugUtils.logOperation(op, "properFrame: ${calculatedFrame}")

                // 09-08-25 The way this got solved may be related to the "bunching" problem
                if (calculatedFrame < 0) {
                    val decision = lateOperationHandler.handleLateOperation(op, calculatedFrame, 0)

                    when (decision) {
                        is LateOperationDecision.Drop -> {
                            // Skip this operation
                        }
                        is LateOperationDecision.Delay -> {
                            coordinatorState.bufferOperation(op, decision.targetFrame)
                        }
                    }
                } else {
                    coordinatorState.bufferOperation(op, calculatedFrame)
                }
            }

            // TEMPORARY DEBUG
            if (operations.isNotEmpty()) {
                log.debug("Renumbered {} operations from old frame to new frame {}",
                    operations.size, newFrame)
            }

            newFrame++
        }

        // Prepare manifests for all frames we have operations for
        val framesToPrepare = newFrame
        for (frame in 0 until framesToPrepare) {
            prepareManifestForFrame(frame)
        }

        log.info("Session: renumbered {} old frames to frames 0-{}, prepared {} manifests",
            operationsByOldFrame.size, newFrame - 1, framesToPrepare)

        return newFrame
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

        // TEMPORARY DEBUG
        log.info("OPERATION_FLOW Extracting from buffer: ${operationsByOldFrame}")

        return operationsByOldFrame
    }

    /**
     * Prepare any pending manifests, occurs following a hard pause and resume,
     * such as when backpressure control is de-activated
     */
    private suspend fun preparePendingManifests() {
        if (coordinatorState.pauseState in setOf(PauseState.REST, PauseState.SOFT)) {
            log.info("Delayed preparing frames from ${coordinatorState.lastPreparedManifest} due to pause ${coordinatorState.pauseState}")
            return
        }

        val framesToPrepare = getFramesToPrepare()

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
     * Handle frame completion event triggered by WorkerFrameCompleteEvent when all workers finish.
     * This is what permits workers to complete the next frame
     */
    private suspend fun onFrameComplete(logicalFrame: Long) {
        log.info("Logical frame {} completed successfully", logicalFrame)

        if (coordinatorState.pauseState in setOf(PauseState.SOFT, PauseState.HARD)) {
            log.info("Completed frame $logicalFrame, stopping due to pause ${coordinatorState.pauseState}")
            return
        }

        markFrameComplete(logicalFrame)

        // Ensure the next manifest always exists before the workers advance
        if (coordinatorState.pauseState != PauseState.REST) {
            prepareUpcomingFrames()

            if (sessionService.needAutoSession()) {
                launch { doAutoSession() }
            }
        }

        log.info("Broadcasting next frame event after completing frame {}", logicalFrame)
        vertx.eventBus().publish(ADDRESS_NEXT_FRAME, JsonObject())

        cleanupCompletedFrame(logicalFrame)
    }

    /**
     * End the current session and begin a new one
     */
    private suspend fun doAutoSession() {
        sessionService.endSession()

        // TODO: Snapshot goes here

        sessionService.initializeSession()

        val eventMessage = eventWaiter.waitForEvent(ADDRESS_SESSION_INITIALIZED)

        val newSessionId = eventMessage.getString("sessionId")
        log.info("Frame coordinator received initial session announcement for $newSessionId")
        coordinatorState.sessionId = newSessionId

        sessionService.startSession()
    }

    private fun markFrameComplete(logicalFrame: Long) {
        coordinatorState.markFrameProcessed(logicalFrame)
        coordinatorState.setFrameInProgress(logicalFrame + 1)
    }

    private suspend fun prepareUpcomingFrames() {
        preparePendingManifests()
    }

    /**
     * Get frames that should be prepared right now based on current state
     */
    fun getFramesToPrepare(): List<Long> {
        val lastPrepared = coordinatorState.lastPreparedManifest

        val wallClockFrame = frameCalculator.getCurrentLogicalFrame(coordinatorState.sessionStartNanos)
        val neededFrame = coordinatorState.frameInProgress + 1  // 09-08-25 - This SEEMS to be problematic at session boundaries

        val targetFrame = max(
            wallClockFrame + frameConfig.normalOperationLookahead,
            neededFrame + frameConfig.normalOperationLookahead
        )

        return if (targetFrame > lastPrepared) {
            ((lastPrepared + 1)..targetFrame).toList()
        } else {
            emptyList()
        }
    }

    private fun cleanupCompletedFrame(logicalFrame: Long) {
        frameManifestBuilder.clearFrameManifests(logicalFrame)
        frameCompletionTracker.clearFrameCompletions(logicalFrame)
    }

    override suspend fun stop() {
        log.info("Stopping FrameCoordinatorVerticle")
        messageQueueOperationConsumer.stop()
        super.stop()
    }
}