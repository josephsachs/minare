package com.minare.core.frames.worker

import com.hazelcast.core.HazelcastException
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.cp.IAtomicLong
import com.hazelcast.map.IMap
import com.minare.core.operation.models.Operation
import com.minare.application.config.FrameConfiguration
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle
import com.minare.worker.coordinator.models.FrameManifest
import com.minare.worker.coordinator.models.OperationCompletion
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
    private val frameConfig: FrameConfiguration
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameWorkerVerticle::class.java)

    private lateinit var workerId: String
    private lateinit var manifestMap: IMap<String, JsonObject>
    private lateinit var completionMap: IMap<String, OperationCompletion>

    // Distributed frame progress from Hazelcast
    private val frameProgress: IAtomicLong by lazy {
        hazelcastInstance.getCPSubsystem().getAtomicLong("frame-progress")
    }

    private var sessionStartTimestamp: Long = 0L  // Wall clock for external communication
    private var processingActive = false

    // Track the current frame processing job for clean cancellation
    private var currentFrameProcessingJob: Job? = null

    companion object {
        const val ADDRESS_WORKER_FRAME_COMPLETE = "minare.coordinator.worker.frame.complete"
        const val ADDRESS_WORKER_FRAME_ERROR = "minare.coordinator.worker.frame.error"
        const val ADDRESS_NEXT_FRAME = "minare.coordinator.next.frame"
    }

    override suspend fun start() {
        log.info("Starting FrameWorkerVerticle")
        vlog.setVerticle(this)

        workerId = config.getString("workerId")
        manifestMap = hazelcastInstance.getMap("frame-manifests")
        completionMap = hazelcastInstance.getMap("operation-completions")

        vertx.eventBus().consumer<JsonObject>(FrameCoordinatorVerticle.ADDRESS_SESSION_START) { msg ->
            launch {
                handleSessionStart(msg.body())
            }
        }

        vertx.eventBus().consumer<JsonObject>(ADDRESS_NEXT_FRAME) { msg ->
            vlog.getEventLogger().trace(
                "NEXT_FRAME_EVENT",
                mapOf(
                    "timestamp" to System.currentTimeMillis()
                )
            )

            launch {
                handleNextFrame()
            }
        }

        log.info("FrameWorkerVerticle started for worker {}", workerId)
    }

    /**
     * Handle new session announcement and start frame processing.
     * Ensures any existing processing is cleanly terminated first.
     */
    private suspend fun handleSessionStart(announcement: JsonObject) {
        val newSessionStart = announcement.getLong("sessionStartTimestamp")
        val startsIn = announcement.getLong("firstFrameStartsIn")
        val frameDuration = announcement.getLong("frameDuration")

        log.debug("New session announced - starts at {} (in {}ms), frame duration {}ms",
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

        sessionStartTimestamp = newSessionStart
        processingActive = true

        // Wait for session start
        delay(startsIn)

        // Now wait for subsequent frame advancement events
        log.info("Worker {} ready for frame advancement events", workerId)
    }

    /**
     * Handle next frame event from coordinator.
     * Pull current frame from Hazelcast and process it.
     */
    private suspend fun handleNextFrame() {
        val frameToProcess = frameProgress.get()

        try {
            processLogicalFrame(frameToProcess)
        } catch (e: Exception) {
            log.error("Error processing frame {}", frameToProcess, e)
            reportFrameError(frameToProcess)
            // Still report completion so coordinator can advance
            reportFrameCompletion(frameToProcess, 0)
        }
    }

    /**
     * Clear any stale manifests or completions for this worker.
     * Called when starting a new session to ensure clean state.
     */
    private fun clearWorkerManifests() {
        try {
            // Clear any completion records for this worker
            val completionKeys = completionMap.keys.filter { key ->
                key.contains(workerId)
            }
            completionKeys.forEach { completionMap.remove(it) }

            log.debug("Cleared {} stale completion records for worker {}",
                completionKeys.size, workerId)
        } catch (e: Exception) {
            log.warn("Error clearing worker state in Hazelcast", e)
        }
    }

    /**
     * Process a single logical frame.
     * Fetches manifest from Hazelcast and processes all assigned operations.
     */
    private suspend fun processLogicalFrame(logicalFrame: Long) {
        val frameStartTime = System.currentTimeMillis()

        try {
            val manifestKey = FrameManifest.makeKey(logicalFrame, workerId)

            // Keep trying until we find the manifest
            var manifestJson = manifestMap[manifestKey]
            var attempt = 0

            while (manifestJson == null) {
                attempt++
                if (attempt == 1) {
                    log.warn("Manifest not found for frame {}, waiting...", logicalFrame)
                } else if (attempt % 20 == 0) { // Log every second at 50ms delays
                    log.warn("Still waiting for manifest for frame {} (attempt {})", logicalFrame, attempt)
                }

                delay(50)
                manifestJson = manifestMap[manifestKey]
            }

            val manifest = FrameManifest.fromJson(manifestJson)
            val operations = manifest.operations

            log.info("Processing logical frame {} with {} operations",
                logicalFrame, operations.size)

            // Process operations sequentially, order matters
            var successCount = 0
            for (operation in operations) {
                if (processOperation(operation, logicalFrame)) {
                    successCount++
                }
            }

            reportFrameCompletion(logicalFrame, successCount)

            val processingTime = System.currentTimeMillis() - frameStartTime
            if (processingTime > frameConfig.frameDurationMs * 0.8) {
                log.warn("Frame {} processing took {}ms ({}% of frame duration)",
                    logicalFrame, processingTime,
                    (processingTime * 100 / frameConfig.frameDurationMs))
            }

        } catch (e: HazelcastException) {
            log.error("Hazelcast error in frame {}", logicalFrame, e)
            reportFrameError(logicalFrame)
            reportFrameCompletion(logicalFrame, 0)

        } catch (e: TimeoutException) {
            log.error("Timeout processing frame {}", logicalFrame, e)
            reportFrameError(logicalFrame)
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
            val op = Operation.fromJson(operation)

            // Add frame number to operation for delta storage
            operation.put("frameNumber", logicalFrame)

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
     * Report a frame processing error
     */
    private fun reportFrameError(logicalFrame: Long) {
        val errorEvent = JsonObject()
            .put("workerId", workerId)
            .put("logicalFrame", logicalFrame)
            .put("timestamp", System.currentTimeMillis())

        vertx.eventBus().send(ADDRESS_WORKER_FRAME_ERROR, errorEvent)
    }

    override suspend fun stop() {
        log.info("Stopping FrameWorkerVerticle")
        processingActive = false

        currentFrameProcessingJob?.cancel()
        currentFrameProcessingJob = null

        super.stop()
    }
}