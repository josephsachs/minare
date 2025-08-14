package com.minare.worker.coordinator

import com.hazelcast.core.HazelcastException
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.operation.Operation
import com.minare.time.FrameConfiguration
import com.minare.time.FrameCalculator
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeoutException
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
    private val frameCalculator: FrameCalculator,
    private val frameConfig: FrameConfiguration
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameWorkerVerticle::class.java)

    private lateinit var workerId: String
    private lateinit var manifestMap: IMap<String, JsonObject>
    private lateinit var completionMap: IMap<String, OperationCompletion>

    private var sessionStartTimestamp: Long = 0L  // Wall clock for external communication
    private var sessionStartNanos: Long = 0L      // Nanos for accurate frame pacing
    private var currentLogicalFrame: Long = -1L
    private var processingActive = false

    // Track the current frame processing job for clean cancellation
    private var currentFrameProcessingJob: Job? = null

    companion object {
        const val ADDRESS_FRAME_MANIFEST = "worker.frame.manifest"
        const val ADDRESS_WORKER_FRAME_COMPLETE = "minare.coordinator.worker.frame.complete"
        const val ADDRESS_WORKER_FRAME_DELAYED = "minare.coordinator.worker.frame.delayed"
        const val ADDRESS_WORKER_FRAME_ERROR = "minare.coordinator.worker.frame.error"
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
            launch {
                handlePause(msg.body())
            }
        }

        log.info("FrameWorkerVerticle started for worker {}", workerId)
    }

    /**
     * Handle pause event by stopping frame processing cleanly.
     */
    private suspend fun handlePause(pauseMessage: JsonObject) {
        val reason = pauseMessage.getString("reason", "unknown")
        log.info("Received pause event: {}", reason)

        // Stop processing
        processingActive = false

        // Cancel the current frame processing job if it exists
        currentFrameProcessingJob?.let { job ->
            log.info("Cancelling frame processing job for pause")
            job.cancel()
            currentFrameProcessingJob = null
        }
    }

    /**
     * Handle new session announcement and start frame processing.
     * Ensures any existing processing is cleanly terminated first.
     */
    private suspend fun handleSessionStart(announcement: JsonObject) {
        val newSessionStart = announcement.getLong("sessionStartTimestamp")
        val startsIn = announcement.getLong("firstFrameStartsIn")
        val frameDuration = announcement.getLong("frameDuration")

        log.info("New session announced - starts at {} (in {}ms), frame duration {}ms",
            newSessionStart, startsIn, frameDuration)

        // CRITICAL: Stop any existing processing first
        if (processingActive || currentFrameProcessingJob != null) {
            log.warn("Stopping existing frame processing before starting new session")
            processingActive = false

            // Cancel and wait for the job to complete
            currentFrameProcessingJob?.let { job ->
                try {
                    job.cancel()
                    job.join()
                } catch (e: Exception) {
                    log.error("Error cancelling previous frame processing job", e)
                }
                currentFrameProcessingJob = null
            }

            // Give a bit more time for cleanup
            delay(frameConfig.frameDurationMs / 2)
        }

        // Clear any stale state in Hazelcast for this worker
        clearWorkerManifests()

        // Reset state for new session
        sessionStartTimestamp = newSessionStart
        currentLogicalFrame = -1L
        processingActive = true

        // Wait for session start
        delay(startsIn)

        // Capture nanoTime at actual session start for frame pacing
        sessionStartNanos = System.nanoTime()

        // Start frame processing loop and track the job
        currentFrameProcessingJob = launchFrameProcessingLoop()
    }

    /**
     * Clear any stale manifests or completions for this worker.
     * Called when starting a new session to ensure clean state.
     */
    private suspend fun clearWorkerManifests() {
        try {
            // Clear any manifests for this worker
            val keysToRemove = mutableListOf<String>()
            manifestMap.keys.forEach { key ->
                if (key.contains(workerId)) {
                    keysToRemove.add(key)
                }
            }

            keysToRemove.forEach { key ->
                manifestMap.remove(key)
            }

            if (keysToRemove.isNotEmpty()) {
                log.info("Cleared {} stale manifests for worker {}", keysToRemove.size, workerId)
            }

            // Clear any completions for this worker
            val completionKeysToRemove = mutableListOf<String>()
            completionMap.keys.forEach { key ->
                if (key.contains(workerId)) {
                    completionKeysToRemove.add(key)
                }
            }

            completionKeysToRemove.forEach { key ->
                completionMap.remove(key)
            }

            if (completionKeysToRemove.isNotEmpty()) {
                log.info("Cleared {} stale completions for worker {}", completionKeysToRemove.size, workerId)
            }
        } catch (e: Exception) {
            log.error("Error clearing worker state in Hazelcast", e)
        }
    }

    /**
     * Launch the frame processing loop with nanoTime-based pacing.
     * Returns the Job so it can be tracked and cancelled if needed.
     */
    private fun launchFrameProcessingLoop(): Job = launch {
        log.info("Starting frame processing loop for session {} (nanos: {})",
            sessionStartTimestamp, sessionStartNanos)

        var logicalFrame = 0L

        while (processingActive) {
            try {
                processLogicalFrame(logicalFrame)

                val nanosUntilNextFrame = frameCalculator.nanosUntilFrame(logicalFrame + 1, sessionStartNanos)

                if (nanosUntilNextFrame > 0) {
                    // We finished early - wait for next frame
                    delay(frameCalculator.nanosToMs(nanosUntilNextFrame))
                } else {
                    val nanosLate = -nanosUntilNextFrame // Detect if lagging

                    if (frameCalculator.isLaggingBeyondThreshold(nanosLate)) {
                        log.error("Worker {} is {} nanoseconds behind schedule", workerId, nanosLate)
                        reportWorkerLagged(logicalFrame, nanosLate)
                    }
                }

                logicalFrame++

            } catch (e: Exception) {
                log.error("Error processing logical frame {}", logicalFrame, e)

                val delayMs = frameCalculator.msUntilFrame(logicalFrame + 1, sessionStartNanos)
                reportFrameError()

                if (delayMs > 0) {
                    delay(delayMs) // Wait until the next frame and resume
                }
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

        } catch (e: HazelcastException) {
            log.error("Hazelcast error in frame {}", logicalFrame, e)
            reportFrameError()
            reportFrameCompletion(logicalFrame, 0)

        } catch (e: TimeoutException) {
            log.error("Timeout processing frame {}", logicalFrame, e)
            reportFrameError()
            reportFrameCompletion(logicalFrame, 0)

        } catch (e: Exception) {
            // After we iron out any framework bugs that could be causing this exception,
            // delete the catch-all for simpler logic
            log.error("Unexpected error in frame {} - this is likely a bug", logicalFrame, e)
            throw e  // Let it crash so we notice and fix it
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

            // Send to the appropriate processor based on action type
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
     * Report frame completion to the coordinator.
     * Includes heartbeat for frames with no operations.
     */
    private fun reportFrameCompletion(logicalFrame: Long, operationsProcessed: Int) {
        val completionEvent = JsonObject()
            .put("workerId", workerId)
            .put("logicalFrame", logicalFrame)
            .put("operationCount", operationsProcessed)
            .put("completedAt", System.currentTimeMillis())

        vertx.eventBus().send(ADDRESS_WORKER_FRAME_COMPLETE, completionEvent)

        log.debug("Reported logical frame {} completion with {} operations",
            logicalFrame, operationsProcessed)
    }

    /**
     * Report that this worker is lagging behind schedule
     */
    private fun reportWorkerLagged(logicalFrame: Long, nanosLate: Long) {
        val lagEvent = JsonObject()
            .put("workerId", workerId)
            .put("logicalFrame", logicalFrame)
            .put("nanosLate", nanosLate)
            .put("msLate", frameCalculator.nanosToMs(nanosLate))
            .put("timestamp", System.currentTimeMillis())

        vertx.eventBus().send(ADDRESS_WORKER_FRAME_DELAYED, lagEvent)
    }

    /**
     * Report a frame processing error
     */
    private fun reportFrameError() {
        val errorEvent = JsonObject()
            .put("workerId", workerId)
            .put("logicalFrame", currentLogicalFrame)
            .put("timestamp", System.currentTimeMillis())

        vertx.eventBus().send(ADDRESS_WORKER_FRAME_ERROR, errorEvent)
    }

    override suspend fun stop() {
        log.info("Stopping FrameWorkerVerticle")
        processingActive = false

        // Cancel frame processing job
        currentFrameProcessingJob?.cancel()
        currentFrameProcessingJob = null

        super.stop()
    }
}