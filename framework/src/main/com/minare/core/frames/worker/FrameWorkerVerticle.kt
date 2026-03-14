package com.minare.core.frames.worker

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.cp.IAtomicLong
import com.hazelcast.map.IMap
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle
import com.minare.core.frames.events.WorkerStartStateSnapshotEvent
import com.minare.core.frames.services.VerticleRegistry
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.exceptions.FrameLoopException
import com.minare.worker.coordinator.models.FrameManifest
import com.minare.worker.coordinator.models.OperationCompletion
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.Job
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import com.google.inject.Inject

/**
 * Worker-side frame processing verticle.
 * Updated for logical frames with nanoTime-based pacing:
 * - Processes frames based on logical numbering
 * - Uses System.nanoTime() for drift-free frame pacing
 * - Immune to wall clock adjustments
 *
 * Each instance generates a unique instanceId ("$deploymentID-$UUID") at startup,
 * matching the pattern used by socket verticles. The instanceId is used for
 * manifest keying, operation dispatch, and frame completion reporting.
 */
class FrameWorkerVerticle @Inject constructor(
    private val vlog: VerticleLogger,
    private val hazelcastInstance: HazelcastInstance,
    private val workerStartStateSnapshotEvent: WorkerStartStateSnapshotEvent,
    private val verticleRegistry: VerticleRegistry,
    private val stateStore: StateStore,
    private val entityFactory: EntityFactory,
    private val reflectionCache: ReflectionCache
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameWorkerVerticle::class.java)
    private val debugTraceLogs: Boolean = false

    private lateinit var instanceId: String
    private lateinit var nodeId: String
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

        nodeId = config.getString("nodeId")
            ?: throw IllegalStateException("FrameWorkerVerticle requires nodeId in config")
        instanceId = config.getString("instanceId")
            ?: throw IllegalStateException("FrameWorkerVerticle requires instanceId in config")

        manifestMap = hazelcastInstance.getMap("frame-manifests")
        completionMap = hazelcastInstance.getMap("operation-completions")

        // Register this instance in the verticle registry
        verticleRegistry.addInstance(instanceId, nodeId)

        workerStartStateSnapshotEvent.register(instanceId)

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

        log.info("FrameWorkerVerticle started (instance: {}, node: {})", instanceId, nodeId)
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

        log.info("Instance {} ready for frame advancement events", instanceId)
    }

    /**
     * Process a single logical frame.
     * Fetches manifest from Hazelcast and processes all assigned operations.
     */
    private suspend fun processLogicalFrame(logicalFrame: Long) {
        val manifestKey = FrameManifest.makeKey(logicalFrame, instanceId)
        val manifestJson = getManifest(manifestKey, logicalFrame)
        val manifest = FrameManifest.fromJson(manifestJson)
        val operations = manifest.operations

        if (debugTraceLogs) {
            log.info("Processing logical frame {} with {} operations",
                logicalFrame, operations.size)
        }

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
     * Process the list of operations.
     *
     * Contiguous members sharing an operationSetId are collected and handed to
     * OperationSetExecutor for sequential execution. Solo operations are processed
     * individually as before.
     *
     * The manifest builder guarantees set members are sorted contiguously by
     * operationSetId then setIndex, so a linear scan suffices.
     */
    private suspend fun processOperations(operations: List<JsonObject>, logicalFrame: Long): Int {
        var successCount = 0
        var i = 0

        while (i < operations.size) {
            val operation = operations[i]
            val setId = operation.getString("operationSetId")

            if (setId != null) {
                val setMembers = operations.drop(i).takeWhile {
                    it.getString("operationSetId") == setId
                }

                val executor = OperationSetExecutor(
                    vertx           = vertx,
                    stateStore      = stateStore,
                    entityFactory   = entityFactory,
                    reflectionCache = reflectionCache,
                    instanceId      = instanceId,
                    completionMap   = completionMap,
                    workerScope     = this
                )

                successCount += executor.execute(setMembers, logicalFrame)
                i += setMembers.size
            } else {
                if (processOperation(operation, logicalFrame)) successCount++
                i++
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
            // Send to this instance's scoped handler
            val processorAddress = "worker.process.${operation.getString("action")}.$instanceId"

            // Produce processing context
            val processingContext = JsonObject()
                .put("operation", operation)
                .put("frameNumber", logicalFrame)

            val result = vertx.eventBus()
                .request<JsonObject>(processorAddress, processingContext)
                .await()

            if (result.body().getBoolean("success", false)) {
                val completionKey = "frame-$logicalFrame:op-$operationId"

                completionMap[completionKey] = OperationCompletion(
                    operationId = operationId,
                    workerId = instanceId
                )

                if (debugTraceLogs) log.trace("Completed operation {} for entity {}", operationId, operation.getString("entityId"))
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
            .put("workerId", instanceId)
            .put("logicalFrame", logicalFrame)
            .put("operationCount", operationsProcessed)
            .put("completedAt", System.currentTimeMillis())

        vertx.eventBus().publish(ADDRESS_WORKER_FRAME_COMPLETE, completionEvent)

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
