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
 * Updated for logical frames:
 * - Processes frames based on logical numbering
 * - Maintains consistent frame pacing using delays
 * - No dependency on wall clock for frame boundaries
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

    // Session state
    private var sessionStartTimestamp: Long = 0L
    private var currentLogicalFrame: Long = -1L
    private var processingActive = false

    companion object {
        const val ADDRESS_FRAME_MANIFEST = "worker.{workerId}.frame.manifest"
    }

    override suspend fun start() {
        log.info("Starting FrameWorkerVerticle")
        vlog.setVerticle(this)

        // Get worker ID from hostname or config
        workerId = System.getenv("HOSTNAME") ?: config.getString("workerId") ?:
                throw IllegalStateException("Worker ID not configured")

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

        // Start frame processing loop
        launchFrameProcessingLoop()
    }

    /**
     * Launch the frame processing loop with consistent pacing.
     */
    private fun launchFrameProcessingLoop() = launch {
        log.info("Starting frame processing loop for session {}", sessionStartTimestamp)

        var nextFrameTime = sessionStartTimestamp
        var logicalFrame = 0L

        while (processingActive) {
            try {
                // Process the current logical frame
                processLogicalFrame(logicalFrame)

                // Calculate next frame time
                nextFrameTime += frameConfig.frameDurationMs
                val now = System.currentTimeMillis()
                val delayMs = nextFrameTime - now

                if (delayMs > 0) {
                    // On schedule - wait for next frame
                    delay(delayMs)
                } else if (delayMs < -frameConfig.frameDurationMs) {
                    // More than one frame behind - we're in trouble
                    log.error("Worker {} is {} frames behind schedule",
                        workerId, (-delayMs / frameConfig.frameDurationMs))
                    // Continue anyway - coordinator will handle if we're too slow
                }

                logicalFrame++

            } catch (e: Exception) {
                log.error("Error processing logical frame {}", logicalFrame, e)
                // Continue with next frame
                delay(100)  // Brief pause before retry
            }
        }

        log.info("Frame processing loop stopped")
    }

    /**
     * Process a single logical frame.
     */
    private suspend fun processLogicalFrame(logicalFrame: Long) {
        currentLogicalFrame = logicalFrame

        log.debug("Processing logical frame {}", logicalFrame)

        // Fetch manifest from distributed map
        val manifestKey = "manifest:$logicalFrame:$workerId"
        val manifest = manifestMap[manifestKey]

        if (manifest == null) {
            log.trace("No manifest found for worker {} in logical frame {}",
                workerId, logicalFrame)
            // Still report completion with 0 operations
            reportFrameCompletion(logicalFrame, 0)
            return
        }

        // Process the operations
        val operations = manifest.getJsonArray("operations", JsonArray())
        val operationCount = operations.size()

        if (operationCount > 0) {
            log.debug("Processing {} operations for logical frame {}",
                operationCount, logicalFrame)
        }

        // Process each operation
        var completedCount = 0
        operations.forEach { op ->
            if (op is JsonObject) {
                val success = processOperation(op, logicalFrame)
                if (success) {
                    completedCount++
                }
            }
        }

        if (operationCount > 0) {
            log.info("Completed {}/{} operations for logical frame {}",
                completedCount, operationCount, logicalFrame)
        }

        // Report frame completion
        reportFrameCompletion(logicalFrame, completedCount)
    }

    /**
     * Process a single operation.
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
     */
    private suspend fun reportFrameCompletion(
        logicalFrame: Long,
        operationCount: Int
    ) {
        try {
            val completion = JsonObject()
                .put("workerId", workerId)
                .put("logicalFrame", logicalFrame)  // Changed from frameStartTime
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

    override suspend fun stop() {
        log.info("Stopping FrameWorkerVerticle")
        processingActive = false
        super.stop()
    }
}