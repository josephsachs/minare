package com.minare.worker.coordinator

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.operation.Operation
import com.minare.time.FrameConfiguration
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * Worker-side frame processing verticle.
 * Updated for logical frames with nanoTime-based pacing:
 * - Processes frames based on logical numbering
 * - Uses System.nanoTime() for drift-free frame pacing
 * - Immune to wall clock adjustments
 */
class FrameWorkerVerticle @Inject constructor(
    private val vlog: VerticleLogger,
    private val hazelcastInstance: HazelcastInstance,
    private val frameConfig: FrameConfiguration
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameWorkerVerticle::class.java)

    private lateinit var workerId: String
    private lateinit var manifestMap: IMap<String, JsonObject>
    private lateinit var completionMap: IMap<String, OperationCompletion>

    // Session state - now tracking both wall clock and nano time
    private var sessionStartTimestamp: Long = 0L  // Wall clock for external communication
    private var sessionStartNanos: Long = 0L      // Nanos for accurate frame pacing
    private var currentLogicalFrame: Long = -1L
    private var processingActive = false

    companion object {
        const val ADDRESS_FRAME_MANIFEST = "worker.{workerId}.frame.manifest"
        const val ADDRESS_LAG_ALERT = "minare.coordinator.worker.lag.alert"
        const val LAG_ALERT_THRESHOLD = 3L  // Alert coordinator if behind by this many frames
    }

    override suspend fun start() {
        log.info("Starting FrameWorkerVerticle")
        vlog.setVerticle(this)

        workerId = config.getString("workerId")

        // Initialize distributed maps
        manifestMap = hazelcastInstance.getMap("frame-manifests")
        completionMap = hazelcastInstance.getMap("operation-completions")

        // Listen for session start announcements
        vertx.eventBus().consumer<JsonObject>(FrameCoordinatorVerticle.ADDRESS_SESSION_START) { msg ->
            launch {
                handleSessionStart(msg.body())
            }
        }

        // Listen for pause events
        vertx.eventBus().consumer<JsonObject>(FrameCoordinatorVerticle.ADDRESS_FRAME_PAUSE) { msg ->
            log.info("Received pause event: {}", msg.body().getString("reason"))
            processingActive = false
        }

        log.info("FrameWorkerVerticle started for worker {}", workerId)
    }

    /**
     * Handle new session announcement and start frame processing.
     * Now captures nanoTime reference for accurate pacing.
     */
    private suspend fun handleSessionStart(announcement: JsonObject) {
        sessionStartTimestamp = announcement.getLong("sessionStartTimestamp")
        val startsIn = announcement.getLong("firstFrameStartsIn")
        val frameDuration = announcement.getLong("frameDuration")

        log.info("New session announced - starts at {} (in {}ms), frame duration {}ms",
            sessionStartTimestamp, startsIn, frameDuration)

        // Reset state for new session
        currentLogicalFrame = -1L
        processingActive = true

        // Wait for session start
        delay(startsIn)

        // Capture nanoTime at actual session start for frame pacing
        sessionStartNanos = System.nanoTime()

        // Start frame processing loop
        launchFrameProcessingLoop()
    }

    /**
     * Launch the frame processing loop with nanoTime-based pacing.
     * This is immune to wall clock adjustments and provides more accurate timing.
     */
    private fun launchFrameProcessingLoop() = launch {
        log.info("Starting frame processing loop for session {} (nanos: {})",
            sessionStartTimestamp, sessionStartNanos)

        var logicalFrame = 0L
        val frameDurationNanos = frameConfig.frameDurationMs * 1_000_000L

        while (processingActive) {
            try {
                val frameStartNanos = System.nanoTime()

                // Process the current logical frame
                processLogicalFrame(logicalFrame)

                // Calculate when next frame should start
                val expectedNextFrameNanos = sessionStartNanos + ((logicalFrame + 1) * frameDurationNanos)
                val currentNanos = System.nanoTime()
                val delayNanos = expectedNextFrameNanos - currentNanos

                if (delayNanos > 0) {
                    // On schedule - wait for next frame
                    delay(delayNanos / 1_000_000L)  // Convert to milliseconds
                } else if (delayNanos < -frameDurationNanos) {
                    // More than one frame behind - log but continue
                    val framesBehind = -delayNanos / frameDurationNanos
                    log.error("Worker {} is {} frames behind schedule", workerId, framesBehind)

                    // Alert coordinator about falling behind
                    if (framesBehind >= LAG_ALERT_THRESHOLD) {
                        alertCoordinatorOfLag(logicalFrame, framesBehind)
                    }

                    // As you noted - no point trying to catch up
                    // The worker is already processing as fast as it can
                }

                logicalFrame++

            } catch (e: Exception) {
                log.error("Error processing logical frame {}", logicalFrame, e)

                // Calculate proper delay to stay on schedule despite error
                val expectedNextFrameNanos = sessionStartNanos + ((logicalFrame + 1) * frameDurationNanos)
                val currentNanos = System.nanoTime()
                val delayNanos = expectedNextFrameNanos - currentNanos

                if (delayNanos > 0) {
                    delay(delayNanos / 1_000_000L)
                }
                // If we're already behind, continue immediately
            }
        }

        log.info("Frame processing loop stopped")
    }

    /**
     * Process a single logical frame.
     * Fetches manifest from Hazelcast and processes all assigned operations.
     */
    private suspend fun processLogicalFrame(logicalFrame: Long) {
        currentLogicalFrame = logicalFrame
        val frameStartTime = System.currentTimeMillis()

        try {
            // Fetch manifest from Hazelcast
            val manifestKey = FrameManifest.makeKey(logicalFrame, workerId)
            val manifestJson = manifestMap[manifestKey]

            if (manifestJson == null) {
                log.debug("No manifest found for logical frame {} - sending heartbeat", logicalFrame)
                reportFrameCompletion(logicalFrame, 0)
                return
            }

            val manifest = FrameManifest.fromJson(manifestJson)
            val operations = manifest.operations

            log.debug("Processing logical frame {} with {} operations",
                logicalFrame, operations.size)

            // Process operations sequentially
            var successCount = 0
            for (operation in operations) {
                if (processOperation(operation, logicalFrame)) {
                    successCount++
                }
            }

            // Report completion
            reportFrameCompletion(logicalFrame, successCount)

            val processingTime = System.currentTimeMillis() - frameStartTime
            if (processingTime > frameConfig.frameDurationMs * 0.8) {
                log.warn("Frame {} processing took {}ms ({}% of frame duration)",
                    logicalFrame, processingTime,
                    (processingTime * 100 / frameConfig.frameDurationMs))
            }

        } catch (e: Exception) {
            log.error("Error processing frame {}", logicalFrame, e)
            // Still report completion to avoid coordinator deadlock
            reportFrameCompletion(logicalFrame, 0)
        }
    }

    /**
     * Process a single operation within a frame.
     * Returns true if successful, false otherwise.
     */
    private suspend fun processOperation(
        operation: JsonObject,
        logicalFrame: Long
    ): Boolean {
        val operationId = operation.getString("id")
        if (operationId == null) {
            log.error("Operation missing ID: {}", operation.encode())
            return false
        }

        try {
            // Convert to Operation for easier handling
            val op = Operation.fromJson(operation)

            // Send to the appropriate processor based on entity type
            val processorAddress = "worker.process.${op.getAction()}"

            val result = vertx.eventBus()
                .request<JsonObject>(processorAddress, operation)
                .await()

            if (result.body().getBoolean("success", false)) {
                // Mark operation as complete in distributed map
                val completionKey = "frame-$logicalFrame:op-$operationId"
                completionMap[completionKey] = OperationCompletion(
                    operationId = operationId,
                    workerId = workerId
                )

                log.trace("Completed operation {} for entity {}",
                    operationId, op.getEntity())
                return true
            } else {
                log.error("Failed to process operation {}: {}",
                    operationId, result.body().getString("error"))
                return false
            }

        } catch (e: Exception) {
            log.error("Error processing operation {}", operationId, e)
            return false
        }
    }

    /**
     * Report frame completion to coordinator.
     * Critical for coordinator to know when to advance frames.
     */
    private suspend fun reportFrameCompletion(
        logicalFrame: Long,
        operationCount: Int
    ) {
        try {
            val completion = JsonObject()
                .put("workerId", workerId)
                .put("logicalFrame", logicalFrame)
                .put("operationCount", operationCount)
                .put("completedAt", System.currentTimeMillis())

            // Send completion to coordinator
            vertx.eventBus().send(
                "minare.coordinator.worker.frame.complete",
                completion
            )

            log.debug("Reported logical frame {} completion with {} operations",
                logicalFrame, operationCount)

        } catch (e: Exception) {
            log.error("Failed to report frame completion", e)
        }
    }

    /**
     * Get current frame processing metrics.
     * Useful for monitoring worker health and performance.
     */
    fun getMetrics(): JsonObject {
        val elapsedNanos = if (sessionStartNanos > 0) {
            System.nanoTime() - sessionStartNanos
        } else {
            0L
        }

        val expectedFrame = if (sessionStartNanos > 0) {
            elapsedNanos / (frameConfig.frameDurationMs * 1_000_000L)
        } else {
            -1L
        }

        return JsonObject()
            .put("workerId", workerId)
            .put("currentLogicalFrame", currentLogicalFrame)
            .put("expectedLogicalFrame", expectedFrame)
            .put("framesBehind", expectedFrame - currentLogicalFrame)
            .put("sessionStartTimestamp", sessionStartTimestamp)
            .put("processingActive", processingActive)
            .put("elapsedSeconds", elapsedNanos / 1_000_000_000L)
    }

    /**
     * Alert coordinator that this worker is falling behind.
     * This allows the coordinator to make system-wide decisions about pausing or recovery.
     */
    private suspend fun alertCoordinatorOfLag(currentFrame: Long, framesBehind: Long) {
        try {
            val alert = JsonObject()
                .put("workerId", workerId)
                .put("currentFrame", currentFrame)
                .put("framesBehind", framesBehind)
                .put("timestamp", System.currentTimeMillis())

            vertx.eventBus().send(
                ADDRESS_LAG_ALERT,
                alert
            )

            log.warn("Alerted coordinator about {} frame lag", framesBehind)
        } catch (e: Exception) {
            log.error("Failed to alert coordinator about lag", e)
        }
    }

    override suspend fun stop() {
        log.info("Stopping FrameWorkerVerticle")
        processingActive = false
        super.stop()
    }
}