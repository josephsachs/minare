package com.minare.nodegraph.verticles

import com.google.inject.Inject
import com.minare.application.config.FrameworkConfig
import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.coordinator.FrameCoordinatorState.Companion.PauseState
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle.Companion.ADDRESS_FRAME_ALL_COMPLETE
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle.Companion.ADDRESS_SESSION_START
import com.minare.core.frames.coordinator.services.FrameManifestBuilder
import com.minare.nodegraph.controller.NodeGraphChannelController
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * Deployed in the coordinator context only.
 * Listens for frame lifecycle events and broadcasts diagnostic data
 * on the metrics channel for subscribed NodeGraph clients.
 *
 * Two broadcast message types:
 *  - `metrics_frame`  — emitted on each frame completion, carries full operation manifest
 *  - `metrics_pause`  — emitted on session start (carries new pause/session state)
 */
class NodeGraphMetricsVerticle @Inject constructor(
    private val coordinatorState: FrameCoordinatorState,
    private val frameManifestBuilder: FrameManifestBuilder,
    private val channelController: NodeGraphChannelController,
    private val frameworkConfig: FrameworkConfig
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(NodeGraphMetricsVerticle::class.java)

    /** frame number → operation count for current session. Reset on session start. */
    private val accumulator = ConcurrentHashMap<Long, Int>()

    /** Largest affinity group (distinct entity IDs on one worker) seen this session. */
    @Volatile private var sessionMaxAffinityGroup = 0

    override suspend fun start() {
        log.info("Starting NodeGraphMetricsVerticle")

        // ── Session start ──
        vertx.eventBus().consumer<JsonObject>(ADDRESS_SESSION_START) { msg ->
            CoroutineScope(vertx.dispatcher()).launch {
                handleSessionStart(msg.body())
            }
        }

        // ── Frame completion ──
        // Fires when all workers have completed a frame.
        // The coordinator's handler runs concurrently but cleanup happens
        // after a delay(frameDuration) + ADDRESS_NEXT_FRAME, so manifests
        // are safe to read here.
        vertx.eventBus().consumer<JsonObject>(ADDRESS_FRAME_ALL_COMPLETE) { msg ->
            CoroutineScope(vertx.dispatcher()).launch {
                val logicalFrame = msg.body().getLong("logicalFrame")
                handleFrameComplete(logicalFrame)
            }
        }

        log.info("NodeGraphMetricsVerticle started, listening for frame events")
    }

    // ───────────────────────────────────────────────
    // Session Start
    // ───────────────────────────────────────────────

    private suspend fun handleSessionStart(announcement: JsonObject) {
        accumulator.clear()
        sessionMaxAffinityGroup = 0

        val metricsChannelId = channelController.getMetricsChannel() ?: return

        val message = JsonObject()
            .put("type", "metrics_pause")
            .put("pauseState", coordinatorState.pauseState.name)
            .put("frameInProgress", coordinatorState.frameInProgress)
            .put("sessionId", announcement.getString("sessionId"))

        channelController.broadcast(metricsChannelId, message)

        log.debug("Broadcast session start metrics for session {}", announcement.getString("sessionId"))
    }

    // ───────────────────────────────────────────────
    // Frame Completion
    // ───────────────────────────────────────────────

    private suspend fun handleFrameComplete(logicalFrame: Long) {
        val metricsChannelId = channelController.getMetricsChannel() ?: return

        // Aggregate operations from all worker manifests for this frame
        val frameManifests = frameManifestBuilder.getFrameManifests(logicalFrame)
        val allOperations = frameManifests.values.flatMap { manifest ->
            manifest.getJsonArray("operations", JsonArray())
                .filterIsInstance<JsonObject>()
        }

        val opsThisFrame = allOperations.size
        accumulator[logicalFrame] = opsThisFrame

        // Derive session stats
        val opCounts = accumulator.values
        val opsAverage = if (opCounts.isNotEmpty()) opCounts.average() else 0.0
        val opsMax = opCounts.maxOrNull() ?: 0

        // Largest affinity group this frame: max distinct entity IDs on a single worker
        val frameMaxGroup = frameManifests.values.maxOfOrNull { manifest ->
            manifest.getJsonArray("operations", JsonArray())
                .filterIsInstance<JsonObject>()
                .mapNotNull { it.getString("entityId") }
                .distinct()
                .size
        } ?: 0
        if (frameMaxGroup > sessionMaxAffinityGroup) sessionMaxAffinityGroup = frameMaxGroup

        // Buffer stats from coordinator state
        val bufferCount = coordinatorState.getTotalBufferedOperations()
        val framesBuffered = coordinatorState.getBufferedFrameCount()
        val highestFrameBuffered = coordinatorState.getHighestBufferedFrame()

        val message = JsonObject()
            .put("type", "metrics_frame")
            .put("frameInProgress", coordinatorState.frameInProgress)
            .put("framesPerSession", frameworkConfig.frames.session.framesPerSession)
            .put("lookahead", frameworkConfig.frames.lookahead)
            .put("pauseState", coordinatorState.pauseState.name)
            .put("opsCurrentFrame", opsThisFrame)
            .put("opsAverage", opsAverage)
            .put("opsMax", opsMax)
            .put("bufferCount", bufferCount)
            .put("framesBuffered", framesBuffered)
            .put("highestFrameBuffered", highestFrameBuffered)
            .put("maxAffinityGroup", sessionMaxAffinityGroup)
            .put("operations", JsonArray(allOperations))

        channelController.broadcast(metricsChannelId, message)
    }

    override suspend fun stop() {
        log.info("Stopping NodeGraphMetricsVerticle")
        accumulator.clear()
    }
}