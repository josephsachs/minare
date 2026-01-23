package com.minare.core.frames.coordinator.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.application.config.FrameworkConfig
import com.minare.controller.EntityController
import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.coordinator.FrameCoordinatorState.Companion.PauseState
import com.minare.core.frames.coordinator.FrameCoordinatorState.Companion.TimelineState
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle.Companion.ADDRESS_FRAME_MANIFESTS_ALL_COMPLETE
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle.Companion.ADDRESS_NEXT_FRAME
import com.minare.core.storage.adapters.RedisDeltaStore
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.EventWaiter
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import kotlin.math.abs

@Singleton
class TimelineService @Inject constructor(
    private val entityController: EntityController,
    private val coordinatorState: FrameCoordinatorState,
    private val frameworkConfig: FrameworkConfig,
    private val redisDeltaStore: RedisDeltaStore,
    private val eventBusUtils: EventBusUtils,
    private val eventWaiter: EventWaiter,
    private val vlog: VerticleLogger,
) {
    private val log = LoggerFactory.getLogger(TimelineService::class.java)
    private val debugTraceLogs: Boolean = true

    companion object {
        val ADDRESS_TIMELINE_DETACH_COMPLETE = "minare.coordinator.timeline.detach.complete"
        val ADDRESS_TIMELINE_STEPPED_FRAMES  = "minare.coordinator.timeline.stepped.frames"
        val ADDRESS_TIMELINE_REPLAY_COMPLETE = "minare.coordinator.timeline.replay.complete"
        val ADDRESS_TIMELINE_RESUME_COMPLETE = "minare.coordinator.timeline.resume.complete"
    }

    /**
     * Detach timeline head and stop frame progress
     * @param traceId Optional trace ID for logging
     */
    suspend fun detach(traceId: String = "") {
        if (debugTraceLogs) vlog.logInfo("Initiating timeline detach, trace ID $traceId")

        if (frameworkConfig.frames.timeline.detach.flushOnDetach) {
            coordinatorState.pauseState = PauseState.REST

            eventWaiter.waitFor(ADDRESS_FRAME_MANIFESTS_ALL_COMPLETE)
        }

        coordinatorState.pauseState =
            if (frameworkConfig.frames.timeline.detach.bufferWhenDetached) PauseState.SOFT else PauseState.HARD

        coordinatorState.timelineState = TimelineState.DETACH

        eventBusUtils.publishWithTracing(
            ADDRESS_TIMELINE_DETACH_COMPLETE,
            JsonObject()
                .put("traceId", traceId)
        )
    }

    /**
     * Step through frame deltas
     * @param frames A positive or negative number
     * @param traceId Optional trace ID for logging
     */
    suspend fun stepFrames(frames: Long, traceId: String = "") {
        if (frames == 0L) {
            if (debugTraceLogs) vlog.logInfo("Frames cannot be 0L, traceId $traceId")
            return
        }

        if (coordinatorState.timelineState != TimelineState.DETACH) {
            if (debugTraceLogs) vlog.logInfo("Cannot resume when timeline head not detached, trace ID $traceId")
            return
        }

        val currentFrame = coordinatorState.frameInProgress
        val newFrame = coordinatorState.frameInProgress + frames
        val reverse = frames < 0

        if (newFrame > currentFrame) {
            if (debugTraceLogs) vlog.logInfo("Cannot step ahead of current frame in progress, trace ID $traceId")
            return
        }

        if (coordinatorState.timelineState == TimelineState.DETACH) {
            if (debugTraceLogs) vlog.logInfo("Stepping $frames frames, trace ID $traceId")
        }

        for (frame in frameRange(currentFrame, newFrame)) {
            executeDeltas(frame, reverse)
            coordinatorState.setTimelineHead(frame)
        }

        eventBusUtils.publishWithTracing(
            ADDRESS_TIMELINE_STEPPED_FRAMES,
            JsonObject()
                .put("frames", frames)
                .put("headPosition", newFrame)
                .put("traceId", traceId)
        )
    }

    /**
     * Create a branch from current timeline head position and resume frame loop
     */
    suspend fun resume(traceId: String = "") {
        if (coordinatorState.timelineState !in (setOf(TimelineState.DETACH, TimelineState.REPLAY))) {
            vlog.logInfo("Cannot resume when timeline head not detached, trace ID $traceId")
            return
        }

        branch()

        if (frameworkConfig.frames.timeline.detach.replayOnResume) { // replay on resume
            replay()
        }

        // Clean up old session state

        coordinatorState.timelineState = TimelineState.PLAY

        eventBusUtils.publishWithTracing(
            ADDRESS_TIMELINE_RESUME_COMPLETE,
            JsonObject()
                .put("traceId", traceId)
        )
    }

    /**
     * Create a new session, adding root and branches metadata for traversal
     * Mark previous deltas as stale from the point of divergence
     */
    private suspend fun branch() {
        // Here be dragons
    }

    /**
     * Replay new session up to previous frameInProgress before resuming
     */
    private suspend fun replay(traceId: String = "") {
        coordinatorState.timelineState = TimelineState.REPLAY

        if (frameworkConfig.frames.timeline.replay.bufferWhileReplay) {
            // Todo: Create another coordinator state setting to decouple allow buffering
            coordinatorState.pauseState = PauseState.SOFT
        }

        if (frameworkConfig.frames.timeline.detach.assignOnResume) {
            // Create our manifests using stale deltas as a source
        }

        // Extract buffered, prepare future manifests up to frameInProgress

        // Restart frame loop
        coordinatorState.pauseState = PauseState.SOFT
        eventBusUtils.publishWithTracing(ADDRESS_NEXT_FRAME, JsonObject())

        eventWaiter.waitFor(ADDRESS_FRAME_MANIFESTS_ALL_COMPLETE)

        eventBusUtils.publishWithTracing(
            ADDRESS_TIMELINE_REPLAY_COMPLETE,
            JsonObject()
                .put("traceId", traceId)
        )
    }

    /**
     * Execute delta changes one frame in either direction
     */
    /**
     * Execute delta changes one frame in either direction
     */
    private suspend fun executeDeltas(frame: Long, reverse: Boolean, traceId: String = "") {
        if (abs(coordinatorState.frameInProgress - frame) > 1) {
            if (debugTraceLogs) {
                vlog.logInfo(
                    "Cannot executeDeltas more than one frame in either direction, trace ID $traceId"
                )
            }
            return
        }

        if (debugTraceLogs) {
            vlog.logInfo(
                "Executing deltas for frame $frame, reverse=$reverse, trace ID $traceId"
            )
        }

        // Fetch deltas for this frame
        val frameData = redisDeltaStore.getByFrame(frame)

        if (frameData == null) {
            if (debugTraceLogs) {
                vlog.logInfo(
                    "No delta data found for frame $frame, trace ID $traceId"
                )
            }
            return
        }

        // Log the structure we received for debugging
        vlog.logInfo(
            "Retrieved frame data structure for frame $frame: ${frameData.encode()}"
        )

        // Try to extract deltas - might be wrapped or might be the array directly
        val deltas = try {
            if (frameData.containsKey("deltas")) {
                frameData.getJsonArray("deltas")
            } else {
                // Maybe it's already an array wrapped in JsonObject?
                JsonArray(frameData.encode())
            }
        } catch (e: Exception) {
            vlog.logVerticleError(
                "Failed to extract deltas from frame data for frame $frame: ${e.message}, " +
                        "data structure: ${frameData.encode()}, trace ID $traceId",
                e
            )
            return
        }

        if (deltas.isEmpty) {
            if (debugTraceLogs) {
                vlog.logInfo(
                    "No deltas found for frame $frame, trace ID $traceId"
                )
            }
            return
        }

        vlog.logInfo(
            "Processing ${deltas.size()} deltas for frame $frame, reverse=$reverse"
        )

        // Apply each delta in the appropriate direction
        for (i in 0 until deltas.size()) {
            val delta = deltas.getJsonObject(i)
            val entityId = delta.getString("entityId")
            val stateToApply = if (reverse) {
                delta.getJsonObject("before")
            } else {
                delta.getJsonObject("after")
            }

            if (debugTraceLogs) {
                vlog.logInfo(
                    "Applying delta for entity $entityId, " +
                            "operation=${delta.getString("operation")}, " +
                            "reverse=$reverse, frame=$frame, " +
                            "state=$stateToApply"
                )
            }

            // TODO: EntityController needs to be injected in constructor
            // Apply state change through entity controller
            entityController.saveState(entityId, stateToApply)
        }

        if (debugTraceLogs) {
            vlog.logInfo(
                "Completed executing ${deltas.size()} deltas for frame $frame, trace ID $traceId"
            )
        }
    }

    private fun frameRange(from: Long, to: Long): LongProgression {
        return if (from < to) {
            ((from + 1)..to)
        } else {
            (from downTo (to + 1))
        }
    }
}