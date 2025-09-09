package com.minare.core.frames.worker

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.cp.IAtomicLong
import com.hazelcast.map.IMap
import com.minare.core.operation.models.Operation
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle
import com.minare.exceptions.FrameLoopException
import com.minare.worker.coordinator.models.FrameManifest
import com.minare.worker.coordinator.models.OperationCompletion
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.Job
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
    private val hazelcastInstance: HazelcastInstance
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameWorkerVerticle::class.java)

    private lateinit var workerId: String
    private lateinit var manifestMap: IMap<String, JsonObject>
    private lateinit var completionMap: IMap<String, OperationCompletion>

    private val frameProgress: IAtomicLong by lazy {
        hazelcastInstance.getCPSubsystem().getAtomicLong("frame-progress")
    }

    private var processingActive = false

    private var currentFrameProcessingJob: Job? = null

    companion object {
        const val ADDRESS_WORKER_FRAME_COMPLETE = "minare.coordinator.worker.frame.complete"
        const val ADDRESS_NEXT_FRAME = "minare.coordinator.next.frame"
    }

    /**
     * Start the worker verticle
     */
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
                processLogicalFrame(frameProgress.get())
            }
        }

        log.info("FrameWorkerVerticle started for worker {}", workerId)
    }

    /**
     * Handle new session announcement and start frame processing.
     * Ensures any existing processing is cleanly terminated first.
     */
    private suspend fun handleSessionStart(announcement: JsonObject) {
        processingActive = false

        currentFrameProcessingJob?.join()
        currentFrameProcessingJob = null

        processingActive = true

        log.info("Worker {} ready for frame advancement events", workerId)
    }

    /**
     * Process a single logical frame.
     * Fetches manifest from Hazelcast and processes all assigned operations.
     */
    private suspend fun processLogicalFrame(logicalFrame: Long) {
        val manifestKey = FrameManifest.makeKey(logicalFrame, workerId)
        val manifestJson = getManifest(manifestKey, logicalFrame)
        val manifest = FrameManifest.fromJson(manifestJson)
        val operations = manifest.operations

        log.info("Processing logical frame {} with {} operations",
            logicalFrame, operations.size)

        var count = processOperations(operations, logicalFrame)

        reportFrameCompletion(logicalFrame, count)
    }

    /**
     * Wait until we can find the manifest, block til we're done.
     */
    private fun getManifest(manifestKey: String, logicalFrame: Long): JsonObject {
        return manifestMap[manifestKey]
            ?: throw FrameLoopException(
                "Manifest not found for frame $logicalFrame. Critical coordinator error - manifests must exist before frame processing."
            )
    }

    /**
     * Process the list of operations, passing each one to processOperation
     * for individual processing.
     */
    private suspend fun processOperations(operations: List<JsonObject>, logicalFrame: Long): Int {
        var successCount = 0

        // Process operations sequentially, order matters
        for (operation in operations) {
            if (processOperation(operation, logicalFrame)) {
                successCount++
            }
        }

        return successCount
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

            // Attach the frame number for marking the replay delta
            operation.put("frameNumber", logicalFrame)

            // Send to the appropriate processor based on action type
            val processorAddress = "worker.process.${op.getAction()}"

            val result = vertx.eventBus()
                .request<JsonObject>(processorAddress, operation)
                .await()

            if (result.body().getBoolean("success", false)) {
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
     * Stop the worker verticle
     */
    override suspend fun stop() {
        log.info("Stopping FrameWorkerVerticle")
        processingActive = false

        currentFrameProcessingJob?.cancel()
        currentFrameProcessingJob = null

        super.stop()
    }
}