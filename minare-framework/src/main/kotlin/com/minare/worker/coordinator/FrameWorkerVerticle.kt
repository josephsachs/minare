package com.minare.worker.coordinator

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.operation.Operation
import com.minare.utils.VerticleLogger
import com.minare.worker.coordinator.FrameCoordinatorVerticle
import com.minare.worker.coordinator.OperationCompletion
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * Worker-side frame processing verticle.
 * Handles frame announcements, fetches manifests, processes operations,
 * and reports completions.
 */
class FrameWorkerVerticle @Inject constructor(
    private val vlog: VerticleLogger,
    private val hazelcastInstance: HazelcastInstance
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameWorkerVerticle::class.java)

    private lateinit var workerId: String
    private lateinit var manifestMap: IMap<String, JsonObject>
    private lateinit var completionMap: IMap<String, OperationCompletion>

    // Track current frame to avoid processing old announcements
    private var currentFrameStart: Long = -1L

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

        // Listen for frame start announcements
        vertx.eventBus().consumer<JsonObject>(FrameCoordinatorVerticle.ADDRESS_FRAME_START) { msg ->
            launch {
                handleFrameStart(msg.body())
            }
        }

        log.info("FrameWorkerVerticle started for worker {}", workerId)
    }

    private suspend fun handleFrameStart(frameInfo: JsonObject) {
        val frameStartTime = frameInfo.getLong("frameStartTime")
        val frameEndTime = frameInfo.getLong("frameEndTime")

        // Ignore if we're already processing a newer frame
        if (frameStartTime <= currentFrameStart) {
            log.debug("Ignoring old frame announcement: {} (current: {})",
                frameStartTime, currentFrameStart)
            return
        }

        currentFrameStart = frameStartTime
        log.info("Starting frame {} processing", frameStartTime)

        try {
            // Fetch our manifest from the distributed map
            val manifestKey = "manifest:$frameStartTime:$workerId"
            val manifest = manifestMap[manifestKey]

            if (manifest == null) {
                log.warn("No manifest found for worker {} in frame {}",
                    workerId, frameStartTime)
                // Still report completion with 0 operations
                reportFrameCompletion(frameStartTime, 0)
                return
            }

            // Process the operations
            val operations = manifest.getJsonArray("operations", JsonArray())
            val operationCount = operations.size()

            log.debug("Processing {} operations for frame {}",
                operationCount, frameStartTime)

            // Process each operation
            var completedCount = 0
            operations.forEach { op ->
                if (op is JsonObject) {
                    val success = processOperation(op, frameStartTime)
                    if (success) {
                        completedCount++
                    }
                }
            }

            log.info("Completed {}/{} operations for frame {}",
                completedCount, operationCount, frameStartTime)

            // Report frame completion
            reportFrameCompletion(frameStartTime, completedCount)

        } catch (e: Exception) {
            log.error("Error processing frame {}", frameStartTime, e)
            // Still try to report what we completed
            reportFrameCompletion(frameStartTime, 0)
        }
    }

    private suspend fun processOperation(
        operation: JsonObject,
        frameStartTime: Long
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
            // This would route to MessageQueueConsumerVerticle or similar
            val processorAddress = "worker.process.${op.getAction()}"

            val result = vertx.eventBus()
                .request<JsonObject>(processorAddress, operation)
                .await()

            if (result.body().getBoolean("success", false)) {
                // Mark operation as complete in distributed map
                val completionKey = "frame-$frameStartTime:op-$operationId"
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

    private suspend fun reportFrameCompletion(
        frameStartTime: Long,
        operationCount: Int
    ) {
        try {
            val completion = JsonObject()
                .put("workerId", workerId)
                .put("frameStartTime", frameStartTime)
                .put("operationCount", operationCount)

            // Send completion to coordinator
            vertx.eventBus().send(
                "minare.coordinator.worker.frame.complete",
                completion
            )

            log.debug("Reported frame {} completion with {} operations",
                frameStartTime, operationCount)

        } catch (e: Exception) {
            log.error("Failed to report frame completion", e)
        }
    }

    override suspend fun stop() {
        log.info("Stopping FrameWorkerVerticle")
        super.stop()
    }
}