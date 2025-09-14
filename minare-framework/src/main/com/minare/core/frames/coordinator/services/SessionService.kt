package com.minare.core.frames.coordinator.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.application.config.FrameConfiguration
import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.coordinator.FrameCoordinatorState.Companion.PauseState
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle.Companion.ADDRESS_FRAME_MANIFESTS_ALL_COMPLETE
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle.Companion.ADDRESS_NEXT_FRAME
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle.Companion.ADDRESS_PREPARE_SESSION_MANIFESTS
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle.Companion.ADDRESS_SESSION_MANIFESTS_PREPARED
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle.Companion.ADDRESS_SESSION_START
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.operation.interfaces.MessageQueue
import com.minare.core.storage.interfaces.SnapshotStore
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.EventWaiter
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.*

@Singleton
class SessionService @Inject constructor(
    private val frameConfig: FrameConfiguration,
    private val coordinatorState: FrameCoordinatorState,
    private val snapshotStore: SnapshotStore,
    private val workerRegistry: WorkerRegistry,
    private val frameManifestBuilder: FrameManifestBuilder,
    private val frameCompletionTracker: FrameCompletionTracker,
    private val messageQueue: MessageQueue,
    private val eventBusUtils: EventBusUtils,
    private val eventWaiter: EventWaiter
) {
    private val log = LoggerFactory.getLogger(SessionService::class.java)

    companion object {
        const val ADDRESS_SESSION_INITIALIZED = "minare.coordinator.session.initialized"
    }

    /**
     * Checks if we meet the condition for an automatic session change.
     */
    suspend fun needAutoSession(): Boolean {
        when (frameConfig.autoSession) {
            FrameConfiguration.Companion.AutoSession.NONE -> {
                return false
            }
            FrameConfiguration.Companion.AutoSession.FRAMES_PER_SESSION -> {
                if (coordinatorState.frameInProgress == frameConfig.framesPerSession) return true
            }
        }

        // Kotlin likes things clear
        return false
    }

    /**
     * End the current session, allowing workers to complete assigned work and
     * then pausing.
     */
    suspend fun endSession() {
        log.info(
            "Initiating session transition at frame {}, concluding session {}",
            coordinatorState.frameInProgress,
            coordinatorState.sessionId
        )

        coordinatorState.pauseState = PauseState.REST

        eventWaiter.waitForEvent(ADDRESS_FRAME_MANIFESTS_ALL_COMPLETE)

        coordinatorState.pauseState = PauseState.SOFT
    }

    /**
     * Begins a new session
     */
    suspend fun initializeSession() {
        val sessionId = UUID.randomUUID().toString()
        log.info("Initializing new session with ID $sessionId")

        val announcementTime = System.currentTimeMillis()
        val sessionStartTime = announcementTime
        val sessionStartNanos = System.nanoTime()

        clearPreviousSessionState()

        eventBusUtils.publishWithTracing(
            ADDRESS_PREPARE_SESSION_MANIFESTS,
            JsonObject()
                .put("sessionStartTime", sessionStartTime)
                .put("sessionStartNanos", sessionStartNanos)
                .put("announcementTime", announcementTime)
        )

        eventWaiter.waitForEvent(ADDRESS_SESSION_MANIFESTS_PREPARED)

        val metadata = createMetadata(sessionId, sessionStartTime, announcementTime)

        publishSessionMessage(sessionId, metadata)
        createSessionCollection(sessionId, metadata)
        announceSessionToWorkers(sessionId, metadata)

        eventBusUtils.publishWithTracing(
            ADDRESS_SESSION_INITIALIZED,
            JsonObject()
                .put("sessionId", sessionId)
        )
    }

    fun startSession() {
        coordinatorState.pauseState = PauseState.UNPAUSED

        eventBusUtils.publishWithTracing(
            ADDRESS_NEXT_FRAME,
            JsonObject()
        )

        log.info("Broadcasting initial next frame event for frame 0 in session {}", coordinatorState.sessionId)
    }

    private fun clearPreviousSessionState() {
        frameManifestBuilder.clearAllManifests()
        frameCompletionTracker.clearAllCompletions()
    }

    private fun createMetadata(sessionId: String, sessionStartTime: Long, announcementTime: Long): JsonObject {
        val activeWorkers = workerRegistry.getActiveWorkers()

        return JsonObject()
            .put("eventType", "SESSION_START")
            .put("sessionId", sessionId)
            .put("sessionStartTimestamp", sessionStartTime)
            .put("announcementTimestamp", announcementTime)
            .put("frameDuration", frameConfig.frameDurationMs)
            .put("workerCount", activeWorkers.size)
            .put("workerIds", JsonArray(activeWorkers.toList()))
            .put("coordinatorInstance", "coordinator-${System.currentTimeMillis()}") // TODO: Use a more stable ID
    }

    private suspend fun publishSessionMessage(sessionId: String, metadata: JsonObject) {
        try {
            messageQueue.send("minare.system.events", JsonArray().add(metadata))

            log.info("Published session start event to Kafka for $sessionId")
        } catch (e: Exception) {
            // Big problem, Kafka cannot receive the announcement
            coordinatorState.pauseState = PauseState.HARD

            log.error("Failed to publish session start event", e)
        }
    }

    private suspend fun createSessionCollection(sessionId: String, metadata: JsonObject) {
        try {
            snapshotStore.create(sessionId, metadata)
        } catch (e: Exception) {
            log.error("Failed to create session collection for snapshot", e)
        }
    }

    private fun announceSessionToWorkers(sessionId: String, metadata: JsonObject) {
        eventBusUtils.publishWithTracing(
            ADDRESS_SESSION_START,
            metadata
        )

        log.info("Announced new session $sessionId with {} workers", workerRegistry.getActiveWorkers().size)
    }
}