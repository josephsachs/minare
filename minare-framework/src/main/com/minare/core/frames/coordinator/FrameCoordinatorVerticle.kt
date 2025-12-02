package com.minare.core.frames.coordinator

import com.minare.application.config.FrameConfiguration
import com.minare.core.frames.coordinator.FrameCoordinatorState.Companion.PauseState
import com.minare.core.frames.coordinator.services.*
import com.minare.core.frames.coordinator.services.SessionService.Companion.ADDRESS_SESSION_INITIALIZED
import com.minare.core.frames.events.WorkerStateSnapshotCompleteEvent
import com.minare.core.frames.services.SnapshotService
import com.minare.core.frames.services.SnapshotService.Companion.ADDRESS_SNAPSHOT_COMPLETE
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.EventWaiter
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.worker.coordinator.events.*
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
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
    private val operationHandler: OperationHandler,
    private val vlog: VerticleLogger,
    private val debug: DebugLogger,
    private val eventBusUtils: EventBusUtils,
    private val eventWaiter: EventWaiter,
    private val workerRegistry: WorkerRegistry,
    private val coordinatorState: FrameCoordinatorState,
    private val messageQueueOperationConsumer: MessageQueueOperationConsumer,
    private val frameManifestBuilder: FrameManifestBuilder,
    private val frameCompletionTracker: FrameCompletionTracker,
    private val sessionService: SessionService,
    private val snapshotService: SnapshotService,
    private val infraAddWorkerEvent: InfraAddWorkerEvent,
    private val infraRemoveWorkerEvent: InfraRemoveWorkerEvent,
    private val workerFrameCompleteEvent: WorkerFrameCompleteEvent,
    private val workerHeartbeatEvent: WorkerHeartbeatEvent,
    private val workerRegisterEvent: WorkerRegisterEvent,
    private val workerHealthChangeEvent: WorkerHealthChangeEvent,
    private val workerStateSnapshotCompleteEvent: WorkerStateSnapshotCompleteEvent
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameCoordinatorVerticle::class.java)

    var manifestTimerId: Long? = null

    companion object {
        const val ADDRESS_SESSION_START = "minare.coordinator.session.start"
        const val ADDRESS_FRAME_ALL_COMPLETE = "minare.coordinator.internal.frame-all-complete"
        const val ADDRESS_FRAME_MANIFESTS_ALL_COMPLETE = "minare.coordinator.internal.frame.manifests-all-complete"
        const val ADDRESS_PREPARE_SESSION_MANIFESTS = "minare.coordinator.prepare-session-manifests"
        const val ADDRESS_SESSION_MANIFESTS_PREPARED = "minare.coordinator.session-manifests-prepared"
        const val ADDRESS_NEXT_FRAME = "minare.coordinator.next.frame"
    }

    override suspend fun start() {
        log.info("Starting FrameCoordinatorVerticle")
        vlog.setVerticle(this)

        setupEventBusConsumers()
        setupOperationConsumer()

        startupSession()
    }

    private fun setupEventBusConsumers() {
        infraAddWorkerEvent.register()
        infraRemoveWorkerEvent.register()

        launch {
            workerHeartbeatEvent.register()
            workerFrameCompleteEvent.register()
            workerRegisterEvent.register()
            workerHealthChangeEvent.register()
            workerStateSnapshotCompleteEvent.register()
        }

        // All workers complete a frame
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_FRAME_ALL_COMPLETE) { msg, traceId ->
            launch {
                val logicalFrame = msg.body().getLong("logicalFrame")
                onFrameComplete(logicalFrame)
            }
        }

        // Used in session initialization
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_PREPARE_SESSION_MANIFESTS) { msg, traceId ->
            launch {
                val sessionStartTime = msg.body().getLong("sessionStartTime")
                val sessionStartNanos = msg.body().getLong("sessionStartNanos")

                val bufferedOperations = operationHandler.extractBuffered()

                coordinatorState.resetSessionState(sessionStartTime, sessionStartNanos)
                coordinatorState.initializeFrameProgress()
                coordinatorState.setFrameInProgress(0)

                prepareManifestForFrame(0)

                for (frame in 0 until operationHandler.assignBuffered(bufferedOperations)) {
                    prepareManifestForFrame(frame)
                }

                eventBusUtils.publishWithTracing(
                    ADDRESS_SESSION_MANIFESTS_PREPARED,
                    JsonObject()
                )
            }
        }
    }

    /**
     * Start the Kafka consumer
     */
    private suspend fun setupOperationConsumer() {
        messageQueueOperationConsumer.start(this)
    }

    /**
     * Extract and assign buffered operations at the rhythm of the frame loop
     */
    private suspend fun startManifestTimer() {
        manifestTimerId = vertx.setPeriodic(frameConfig.frameDurationMs) {
            launch {
                val pauseState = coordinatorState.pauseState

                if (pauseState in setOf(PauseState.REST, PauseState.SOFT)) {
                    debug.log(DebugType.COORDINATOR_MANIFEST_TIMER_BLOCKED_TICK, listOf(pauseState))
                    return@launch
                }

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
        coordinatorState.pauseState = PauseState.SOFT

        sessionService.initializeSession()

        val eventMessage = eventWaiter.waitFor(ADDRESS_SESSION_INITIALIZED)

        val newSessionId = eventMessage.getString("sessionId")

        coordinatorState.sessionId = newSessionId

        startManifestTimer()
        sessionService.startSession()
    }

    /**
     * Prepare any pending manifests, occurs following a hard pause and resume,
     * such as when backpressure control is de-activated
     */
    private suspend fun preparePendingManifests() {
        val pauseState = coordinatorState.pauseState
        val lastPreparedManifest = coordinatorState.lastPreparedManifest

        if (coordinatorState.pauseState in setOf(PauseState.REST, PauseState.SOFT)) {
            debug.log(DebugType.COORDINATOR_PREPARE_PENDING_MANIFESTS, listOf(lastPreparedManifest, pauseState))
            return
        }

        for (frame in getPendingFrames()) {
            prepareManifestForFrame(frame)
        }
    }

    /**
     * Get frames that should be prepared right now based on current state
     */
    fun getPendingFrames(): List<Long> {
        val nanotimeFrame = frameCalculator.getCurrentLogicalFrame(coordinatorState.sessionStartNanos)
        val nextFrame = coordinatorState.frameInProgress + 1

        // Prepare the greater of frames elapsed since session start or
        // next frame, plus configurable lookahead window
        val targetFrame = max(
            nanotimeFrame + frameConfig.normalOperationLookahead,
            nextFrame + frameConfig.normalOperationLookahead
        )

        val lastPrepared = coordinatorState.lastPreparedManifest

        // If targetFrame is ahead of the last prepared manifest, return the
        // difference as a list, otherwise nothing to prepare now
        return if (targetFrame > lastPrepared) {
            ((lastPrepared + 1)..targetFrame).toList()
        } else {
            emptyList()
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
    }

    /**
     * Handle frame completion event triggered by WorkerFrameCompleteEvent when all workers finish.
     * This is what permits workers to complete the next frame
     */
    private suspend fun onFrameComplete(logicalFrame: Long) {
        debug.log(DebugType.COORDINATOR_ON_FRAME_COMPLETE_CALLED, listOf(logicalFrame))

        val pauseState = coordinatorState.pauseState

        if (pauseState in setOf(PauseState.SOFT, PauseState.HARD)) {
            debug.log(DebugType.COORDINATOR_ON_FRAME_COMPLETE_BLOCKED, listOf(logicalFrame, pauseState))
            return
        }

        markFrameComplete(logicalFrame)

        if (pauseState != PauseState.REST) {
            preparePendingManifests()   // Ensure the next always exists before workers advance

            if (sessionService.needAutoSession()) {
                launch {
                    doAutoSession()
                }
            }
        }

        delay(getRemainingMs())

        vertx.eventBus().publish(
            ADDRESS_NEXT_FRAME,
            JsonObject().put("logicalFrame", logicalFrame)
        )
        debug.log(DebugType.COORDINATOR_NEXT_FRAME_EVENT, listOf(logicalFrame))

        cleanupCompletedFrame(logicalFrame)
    }

    private suspend fun getRemainingMs(): Long {
        return frameCalculator.msUntilFrameEnd(
            coordinatorState.frameInProgress,
            coordinatorState.sessionStartNanos
        )
    }

    /**
     * End the current session and begin a new one
     */
    private suspend fun doAutoSession() {
        sessionService.endSession()

        val oldSessionId = coordinatorState.sessionId

        snapshotService.doSnapshot(oldSessionId)

        eventWaiter.waitFor(ADDRESS_SNAPSHOT_COMPLETE)

        sessionService.initializeSession()

        val eventMessage = eventWaiter.waitFor(ADDRESS_SESSION_INITIALIZED)

        val newSessionId = eventMessage.getString("sessionId")

        debug.log(DebugLogger.Companion.DebugType.COORDINATOR_SESSION_ANNOUNCEMENT, listOf(newSessionId))

        coordinatorState.sessionId = newSessionId

        sessionService.startSession()
    }

    /**
     * Advance frameInProgress
     */
    private fun markFrameComplete(logicalFrame: Long) {
        coordinatorState.setFrameInProgress(logicalFrame + 1)
    }

    /**
     * Cleanup the frame manifest and completions
     */
    private fun cleanupCompletedFrame(logicalFrame: Long) {
        frameManifestBuilder.clearFrameManifests(logicalFrame)
        frameCompletionTracker.clearFrameCompletions(logicalFrame)
    }

    /**
     * Stop the coordinator
     */
    override suspend fun stop() {
        log.info("Stopping FrameCoordinatorVerticle")
        messageQueueOperationConsumer.stop()
        super.stop()
    }
}