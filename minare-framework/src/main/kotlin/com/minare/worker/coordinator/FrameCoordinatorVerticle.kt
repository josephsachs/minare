package com.minare.worker.coordinator

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.worker.coordinator.FrameCoordinatorState
import com.minare.time.FrameConfiguration
import com.minare.time.TimeService
import com.minare.utils.VerticleLogger
import com.minare.worker.coordinator.events.InfraAddWorkerEvent
import com.minare.worker.coordinator.events.InfraRemoveWorkerEvent
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject
import com.minare.worker.coordinator.events.WorkerFrameCompleteEvent
import com.minare.worker.coordinator.events.WorkerHeartbeatEvent
import com.minare.worker.coordinator.events.WorkerRegisterEvent
import kotlin.math.abs

/**
 * Frame Coordinator - The central orchestrator for frame-based processing.
 *
 * Responsibilities:
 * - Consume operations from Kafka
 * - Group operations into frames based on timing
 * - Write frame manifests to distributed map
 * - Track frame completion across all workers
 * - Handle worker failures and frame recovery
 */
class FrameCoordinatorVerticle @Inject constructor(
    private val frameConfig: FrameConfiguration,
    private val timeService: TimeService,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry,
    private val coordinatorState: FrameCoordinatorState,
    private val hazelcastInstance: HazelcastInstance,
    private val infraAddWorkerEvent: InfraAddWorkerEvent,
    private val infraRemoveWorkerEvent: InfraRemoveWorkerEvent,
    private val workerFrameCompleteEvent: WorkerFrameCompleteEvent,
    private val workerHeartbeatEvent: WorkerHeartbeatEvent,
    private val workerRegisterEvent: WorkerRegisterEvent
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameCoordinatorVerticle::class.java)

    // Kafka consumer
    private var kafkaConsumer: KafkaConsumer<String, String>? = null

    // Distributed maps for frame coordination
    private lateinit var manifestMap: IMap<String, JsonObject>
    private lateinit var completionMap: IMap<String, OperationCompletion>

    companion object {
        // EventBus addresses
        const val ADDRESS_FRAME_START = "minare.coordinator.frame.start"
        const val ADDRESS_FRAME_PAUSE = "minare.coordinator.frame.pause"
        const val ADDRESS_FRAME_RESUME = "minare.coordinator.frame.resume"
        const val ADDRESS_FRAME_ALL_COMPLETE = "minare.coordinator.internal.frame-all-complete"

        // Configuration
        const val OPERATIONS_TOPIC = "minare.operations"
        const val WORKER_HEARTBEAT_TIMEOUT = 15000L
        const val FRAME_STARTUP_GRACE_PERIOD = 5000L
        const val RECOVERY_TIMEOUT = 5000L
    }

    override suspend fun start() {
        log.info("Starting FrameCoordinatorVerticle")
        vlog.setVerticle(this)

        // Initialize distributed maps
        manifestMap = hazelcastInstance.getMap("frame-manifests")
        completionMap = hazelcastInstance.getMap("operation-completions")

        setupEventBusConsumers()
        setupKafkaConsumer()

        // Wait for initial workers before starting frames
        vertx.setTimer(FRAME_STARTUP_GRACE_PERIOD) {
            launch {
                if (coordinatorState.shouldStartFrameLoop()) {
                    startFirstFrame()
                } else {
                    log.warn("Insufficient workers to start frame processing")
                }
            }
        }
    }

    private fun setupEventBusConsumers() {
        // Infrastructure commands
        infraAddWorkerEvent.register()
        infraRemoveWorkerEvent.register()

        // Worker lifecycle
        launch {
            workerRegisterEvent.register()
            workerHeartbeatEvent.register()
            workerFrameCompleteEvent.register()
        }

        // Internal event for when all workers complete a frame
        vertx.eventBus().consumer<JsonObject>(ADDRESS_FRAME_ALL_COMPLETE) { msg ->
            launch {
                val frameStartTime = msg.body().getLong("frameStartTime")
                onFrameComplete(frameStartTime)
            }
        }
    }

    private suspend fun setupKafkaConsumer() {
        val config = mutableMapOf<String, String>()

        // Kafka configuration
        val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"
        config["bootstrap.servers"] = bootstrapServers
        config["group.id"] = "minare-coordinator"
        config["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        config["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        config["enable.auto.commit"] = "true"
        config["auto.commit.interval.ms"] = "1000"
        config["auto.offset.reset"] = "latest"

        kafkaConsumer = KafkaConsumer.create<String, String>(vertx, config)

        // Subscribe to operations topic
        kafkaConsumer!!.subscribe(OPERATIONS_TOPIC).await()

        // Start consuming
        kafkaConsumer!!.handler { record ->
            if (!coordinatorState.isPaused) {
                try {
                    val operations = JsonArray(record.value())

                    // Buffer operations by their frame
                    operations.forEach { op ->
                        if (op is JsonObject) {
                            val timestamp = op.getLong("timestamp") ?: System.currentTimeMillis()
                            val frameStart = coordinatorState.getFrameStartTime(timestamp)

                            coordinatorState.bufferOperation(op, frameStart)
                        }
                    }
                } catch (e: Exception) {
                    log.error("Error processing Kafka record", e)
                }
            }
        }

        log.info("Kafka consumer started, subscribed to {}", OPERATIONS_TOPIC)
    }

    private suspend fun startFirstFrame() {
        log.info("Starting frame loop with duration {}ms", frameConfig.frameDurationMs)

        // Calculate first frame start aligned to clock
        coordinatorState.currentFrameStart = coordinatorState.calculateNextFrameStart()

        // Execute first frame
        executeFrame()
    }

    private suspend fun executeFrame() {
        if (coordinatorState.isPaused) {
            log.info("Frame processing is paused, not executing frame")
            return
        }

        val frameStartTime = coordinatorState.currentFrameStart
        val frameEndTime = frameStartTime + frameConfig.frameDurationMs

        log.debug("Executing frame starting at {}", frameStartTime)

        // Update state to indicate frame is in progress
        coordinatorState.setFrameInProgress(frameStartTime)

        try {
            // 1. Check worker health
            workerRegistry.updateWorkerHealth(WORKER_HEARTBEAT_TIMEOUT)

            val activeWorkers = workerRegistry.getActiveWorkers()
            if (activeWorkers.isEmpty()) {
                log.error("No active workers available for frame {}", frameStartTime)
                pauseFrameProcessing("No active workers")
                return
            }

            // 2. Get operations for this frame window
            val frameOperations = coordinatorState.extractFrameOperations(frameStartTime)

            if (frameOperations.isNotEmpty()) {
                // Distribute operations and write to distributed map
                val assignments = distributeOperations(frameOperations, activeWorkers)
                writeManifestsToMap(frameStartTime, frameEndTime, assignments)
            } else {
                // Even with no operations, workers need frame notification
                writeEmptyManifests(frameStartTime, frameEndTime, activeWorkers)
            }

            // 3. Broadcast frame timing to all workers
            broadcastFrameStart(frameStartTime, frameEndTime)

            // 4. Start deadline monitor
            startFrameDeadlineMonitor(frameStartTime, frameEndTime)

        } catch (e: Exception) {
            log.error("Error executing frame {}", frameStartTime, e)
            pauseFrameProcessing("Frame execution error: ${e.message}")
        }
    }

    private fun distributeOperations(
        operations: List<JsonObject>,
        workers: Set<String>
    ): Map<String, List<JsonObject>> {
        // Distribute operations using consistent hashing
        val workerList = workers.toList().sorted()

        return operations.groupBy { op ->
            // Use entity ID for consistent hashing
            val entityId = op.getString("entityId") ?: ""

            val hash = abs(entityId.hashCode())
            workerList[hash % workerList.size]
        }
    }

    private fun writeManifestsToMap(
        frameStartTime: Long,
        frameEndTime: Long,
        assignments: Map<String, List<JsonObject>>
    ) {
        assignments.forEach { (workerId, operations) ->
            val manifest = JsonObject()
                .put("workerId", workerId)
                .put("frameStartTime", frameStartTime)
                .put("frameEndTime", frameEndTime)
                .put("operations", JsonArray(operations))

            val key = "manifest:$frameStartTime:$workerId"
            manifestMap[key] = manifest

            log.debug("Wrote manifest for worker {} with {} operations",
                workerId, operations.size)
        }
    }

    private fun writeEmptyManifests(
        frameStartTime: Long,
        frameEndTime: Long,
        workers: Set<String>
    ) {
        workers.forEach { workerId ->
            val manifest = JsonObject()
                .put("workerId", workerId)
                .put("frameStartTime", frameStartTime)
                .put("frameEndTime", frameEndTime)
                .put("operations", JsonArray())

            val key = "manifest:$frameStartTime:$workerId"
            manifestMap[key] = manifest
        }
    }

    private fun broadcastFrameStart(frameStartTime: Long, frameEndTime: Long) {
        val frameStart = JsonObject()
            .put("frameStartTime", frameStartTime)
            .put("frameEndTime", frameEndTime)
            .put("frameDuration", frameConfig.frameDurationMs)

        vertx.eventBus().publish(ADDRESS_FRAME_START, frameStart)
        log.info("Broadcast frame start for frame {}", frameStartTime)
    }

    private fun startFrameDeadlineMonitor(frameStartTime: Long, frameEndTime: Long) {
        // Monitor at 80% of frame duration
        val checkDelay = (frameConfig.frameDurationMs * 0.8).toLong()

        vertx.setTimer(checkDelay) {
            launch {
                checkFrameProgress(frameStartTime)
            }
        }
    }

    private suspend fun checkFrameProgress(frameStartTime: Long) {
        val completed = coordinatorState.getCompletedWorkers(frameStartTime)
        val expected = workerRegistry.getActiveWorkers()
        val missing = expected - completed

        if (missing.isNotEmpty()) {
            log.warn("Frame {} progress check: {} workers incomplete at 80% mark",
                frameStartTime, missing.size)

            // Give final warning but don't pause yet
            // The frame deadline handler will deal with actual timeout
        }
    }

    private suspend fun onFrameComplete(frameStartTime: Long) {
        log.info("Frame {} completed successfully", frameStartTime)

        // Clear distributed maps for this frame
        clearFrameMaps(frameStartTime)

        // Calculate next frame time
        coordinatorState.currentFrameStart = frameStartTime +
                frameConfig.frameDurationMs + frameConfig.frameOffsetMs

        // Check if we're behind schedule
        val now = System.currentTimeMillis()
        val drift = now - coordinatorState.currentFrameStart

        if (drift > frameConfig.frameOffsetMs) {
            log.warn("Frame timing drift detected: {}ms behind schedule", drift)
        }

        // Execute next frame
        executeFrame()
    }

    private fun clearFrameMaps(frameStartTime: Long) {
        // Clear manifests
        val manifestKeys = manifestMap.keys
            .filter { it.startsWith("manifest:$frameStartTime:") }

        manifestKeys.forEach { manifestMap.remove(it) }

        // Clear completions
        val completionKeys = completionMap.keys
            .filter { it.startsWith("frame-$frameStartTime:") }

        completionKeys.forEach { completionMap.remove(it) }

        log.debug("Cleared {} manifests and {} completions for frame {}",
            manifestKeys.size, completionKeys.size, frameStartTime)
    }

    private suspend fun pauseFrameProcessing(reason: String) {
        log.warn("Pausing frame processing: {}", reason)
        coordinatorState.isPaused = true

        vertx.eventBus().publish(ADDRESS_FRAME_PAUSE, JsonObject()
            .put("reason", reason)
            .put("timestamp", System.currentTimeMillis())
        )
    }

    private suspend fun attemptFrameRecovery(frameStartTime: Long, missingWorkers: Set<String>) {
        log.info("Attempting recovery for frame {} with missing workers: {}",
            frameStartTime, missingWorkers)

        // Give workers a chance to recover
        delay(RECOVERY_TIMEOUT)

        // Check if missing workers are back
        val stillMissing = missingWorkers.filter { workerId ->
            val state = workerRegistry.getWorkerState(workerId)
            state == null || state.status != WorkerRegistry.WorkerStatus.ACTIVE
        }

        if (stillMissing.isNotEmpty()) {
            // Check distributed map for completed operations
            val incompleteOps = getIncompleteOperations(frameStartTime, stillMissing)

            if (incompleteOps.isNotEmpty()) {
                log.warn("Found {} incomplete operations from failed workers",
                    incompleteOps.size)

                // Redistribute to healthy workers
                val healthyWorkers = workerRegistry.getActiveWorkers() - stillMissing
                if (healthyWorkers.isNotEmpty()) {
                    val reassignments = distributeOperations(incompleteOps, healthyWorkers)
                    writeManifestsToMap(frameStartTime,
                        frameStartTime + frameConfig.frameDurationMs, reassignments)

                    log.info("Redistributed operations to {} healthy workers",
                        healthyWorkers.size)
                }
            }
        }
    }

    private fun getIncompleteOperations(
        frameStartTime: Long,
        workerIds: Collection<String>
    ): List<JsonObject> {
        val incomplete = mutableListOf<JsonObject>()

        workerIds.forEach { workerId ->
            // Get the manifest for this worker
            val manifestKey = "manifest:$frameStartTime:$workerId"
            val manifest = manifestMap[manifestKey]

            if (manifest != null) {
                val operations = manifest.getJsonArray("operations", JsonArray())

                // Check which operations were not completed
                operations.forEach { op ->
                    if (op is JsonObject) {
                        val opId = op.getString("id")
                        val completionKey = "frame-$frameStartTime:op-$opId"

                        if (!completionMap.containsKey(completionKey)) {
                            incomplete.add(op)
                        }
                    }
                }
            }
        }

        return incomplete
    }

    override suspend fun stop() {
        kafkaConsumer?.close()?.await()
        super.stop()
    }
}