package com.minare.worker.coordinator

import com.minare.coordinator.WorkerRegistry
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
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import kotlin.com.minare.worker.coordinator.events.WorkerFrameCompleteEvent
import kotlin.com.minare.worker.coordinator.events.WorkerFrameCompleteEvent.Companion.ADDRESS_WORKER_FRAME_COMPLETE
import kotlin.com.minare.worker.coordinator.events.WorkerHeartbeatEvent
import kotlin.com.minare.worker.coordinator.events.WorkerHeartbeatEvent.Companion.ADDRESS_WORKER_HEARTBEAT
import kotlin.com.minare.worker.coordinator.events.WorkerRegisterEvent
import kotlin.math.abs

/**
 * Frame Coordinator - The central orchestrator for frame-based processing.
 *
 * Responsibilities:
 * - Consume operations from Kafka
 * - Group operations into frames based on timing
 * - Distribute frame manifests to workers
 * - Track frame completion across all workers
 * - Handle worker failures and frame recovery
 */
class FrameCoordinatorVerticle @Inject constructor(
    private val frameConfig: FrameConfiguration,
    private val timeService: TimeService,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry,
    private val infraAddWorkerEvent: InfraAddWorkerEvent,
    private val infraRemoveWorkerEvent: InfraRemoveWorkerEvent,
    private val workerRegisterEvent: WorkerRegisterEvent,
    private val workerHeartbeatEvent: WorkerHeartbeatEvent,
    private val workerFrameCompleteEvent: WorkerFrameCompleteEvent
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameCoordinatorVerticle::class.java)

    // Frame tracking
    private var currentFrameStart = 0L
    private var nextFrameTimerId: Long? = null
    private var isPaused = false

    // Operations buffered by frame start time
    private val operationsByFrame = ConcurrentHashMap<Long, MutableList<JsonObject>>()

    // Frame assignments for tracking and recovery
    private val frameAssignments = ConcurrentHashMap<Long, Map<String, List<JsonObject>>>()

    // Completion tracking by frame start time
    private val frameCompletions = ConcurrentHashMap<String, FrameCompletion>()

    // Kafka consumer
    private var kafkaConsumer: KafkaConsumer<String, String>? = null

    companion object {
        // EventBus addresses
        const val ADDRESS_FRAME_START = "minare.coordinator.frame.start"
        const val ADDRESS_FRAME_PAUSE = "minare.coordinator.frame.pause"
        const val ADDRESS_FRAME_RESUME = "minare.coordinator.frame.resume"

        // Configuration
        const val OPERATIONS_TOPIC = "minare.operations"
        const val WORKER_HEARTBEAT_TIMEOUT = 15000L
        const val FRAME_STARTUP_GRACE_PERIOD = 5000L
        const val RECOVERY_TIMEOUT = 5000L
    }

    data class FrameCompletion(
        val workerId: String,
        val frameStartTime: Long,
        val operationCount: Int,
        val completedAt: Long
    )

    data class FrameManifest(
        val frameStartTime: Long,
        val frameEndTime: Long,
        val operations: List<JsonObject>
    )

    override suspend fun start() {
        log.info("Starting FrameCoordinatorVerticle")
        vlog.setVerticle(this)

        registerEventBusConsumers()

        setupKafkaConsumer()

        // Wait for initial workers before starting frames
        vertx.setTimer(FRAME_STARTUP_GRACE_PERIOD) {
            launch {
                if (workerRegistry.hasMinimumWorkers()) {
                    ensureFrameLoopRunning()
                } else {
                    log.warn("Insufficient workers to start frame processing")
                }
            }
        }
    }

    private fun registerEventBusConsumers() {
        // Infrastructure commands
        infraAddWorkerEvent.register()
        infraRemoveWorkerEvent.register()

        // Worker lifecycle
        launch {
            workerRegisterEvent.register()
            workerHeartbeatEvent.register()
            workerFrameCompleteEvent.register()
        }

        //vertx.eventBus().consumer<JsonObject>(ADDRESS_WORKER_REGISTER) { msg ->
        //    launch {
        //        handleWorkerRegistration(msg.body())
        //    }
        //}

        //vertx.eventBus().consumer<JsonObject>(ADDRESS_WORKER_HEARTBEAT) { msg ->
        //    handleWorkerHeartbeat(msg.body())
        //}

        //vertx.eventBus().consumer<JsonObject>(ADDRESS_WORKER_FRAME_COMPLETE) { msg ->
        //    handleFrameCompletion(msg.body())
        //}
    }

    private suspend fun setupKafkaConsumer() {
        val config = mutableMapOf<String, String>()

        // Kafka configuration
        val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?:
            throw IllegalStateException("KAFKA_BOOTSTRAP_SERVERS environment variable is required")

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
            if (!isPaused) {
                try {
                    val operations = JsonArray(record.value())

                    // Buffer operations by their frame
                    operations.forEach { op ->
                        if (op is JsonObject) {
                            val timestamp = op.getLong("timestamp") ?: System.currentTimeMillis()
                            val frameStart = getFrameStartTime(timestamp)

                            operationsByFrame.computeIfAbsent(frameStart) {
                                mutableListOf()
                            }.add(op)
                        }
                    }
                } catch (e: Exception) {
                    log.error("Error processing Kafka record", e)
                }
            }
        }

        log.info("Kafka consumer started, subscribed to {}", OPERATIONS_TOPIC)
    }

    private suspend fun handleWorkerRegistration(message: JsonObject) {
        val workerId = message.getString("workerId")

        val registered = workerRegistry.registerWorker(workerId)

        if (registered) {
            // If we were waiting for workers, check if we can start now
            if (!isPaused && workerRegistry.hasMinimumWorkers()) {
                ensureFrameLoopRunning()
            }
        }
    }

    private fun handleWorkerHeartbeat(message: JsonObject) {
        val workerId = message.getString("workerId")
        workerRegistry.updateHeartbeat(workerId)
    }

    private fun handleFrameCompletion(message: JsonObject) {
        val workerId = message.getString("workerId")
        val frameStartTime = message.getLong("frameStartTime")
        val operationCount = message.getInteger("operationCount", 0)

        frameCompletions[workerId] = FrameCompletion(
            workerId = workerId,
            frameStartTime = frameStartTime,
            operationCount = operationCount,
            completedAt = System.currentTimeMillis()
        )

        // Track in worker registry
        workerRegistry.recordFrameCompletion(workerId, frameStartTime)

        log.debug("Worker {} completed frame {} with {} operations",
            workerId, frameStartTime, operationCount)
    }

    private suspend fun ensureFrameLoopRunning() {
        if (nextFrameTimerId == null) {
            log.info("No active frame loop detected, starting...")
            startFrameLoop()
        } else {
            log.debug("Frame loop already running")
        }
    }

    private suspend fun startFrameLoop() {
        log.info("Starting frame loop with duration {}ms", frameConfig.frameDurationMs)

        // Calculate first frame start aligned to clock
        currentFrameStart = calculateNextFrameStart()

        // Schedule first frame
        scheduleNextFrame()
    }

    private fun calculateNextFrameStart(): Long {
        val now = System.currentTimeMillis()
        val frameLength = frameConfig.frameDurationMs + frameConfig.frameOffsetMs

        // Align to next frame boundary
        return ((now / frameLength) + 1) * frameLength
    }

    private fun getFrameStartTime(timestamp: Long): Long {
        val frameLength = frameConfig.frameDurationMs + frameConfig.frameOffsetMs
        return (timestamp / frameLength) * frameLength
    }

    private fun scheduleNextFrame() {
        val delay = currentFrameStart - System.currentTimeMillis()

        if (delay > 0) {
            nextFrameTimerId = vertx.setTimer(delay) {
                nextFrameTimerId = null  // Clear timer ID before executing
                launch {
                    executeFrame()
                }
            }
        } else {
            // We're behind schedule
            nextFrameTimerId = null
            launch {
                executeFrame()
            }
        }
    }

    private suspend fun executeFrame() {
        if (isPaused) {
            // Reschedule for next frame
            currentFrameStart += frameConfig.frameDurationMs + frameConfig.frameOffsetMs
            scheduleNextFrame()
            return
        }

        val frameStartTime = currentFrameStart
        val frameEndTime = frameStartTime + frameConfig.frameDurationMs

        log.debug("Executing frame starting at {}", frameStartTime)

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
            val frameOperations = operationsByFrame.remove(frameStartTime) ?: emptyList()

            if (frameOperations.isNotEmpty()) {
                // Create assignments and store for recovery
                val assignments = distributeOperations(frameOperations, activeWorkers)
                frameAssignments[frameStartTime] = assignments

                // Distribute to workers
                distributeFrameManifest(frameStartTime, frameEndTime, assignments)
            }

            // 3. Broadcast frame timing
            broadcastFrameStart(frameStartTime, frameEndTime)

            // 4. Wait for completions during frame offset period
            val deadline = frameEndTime +
                    (frameConfig.frameOffsetMs * frameConfig.coordinationWaitPeriod).toLong()

            waitForFrameCompletions(frameStartTime, activeWorkers, deadline)

        } finally {
            // Schedule next frame
            currentFrameStart += frameConfig.frameDurationMs + frameConfig.frameOffsetMs
            scheduleNextFrame()
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
            val entityId = op.getJsonObject("values")?.getString("entityId")
                ?: op.getString("entityId")
                ?: ""

            val hash = abs(entityId.hashCode())
            workerList[hash % workerList.size]
        }
    }

    private fun distributeFrameManifest(
        frameStartTime: Long,
        frameEndTime: Long,
        assignments: Map<String, List<JsonObject>>
    ) {
        // Send manifest to each worker
        assignments.forEach { (workerId, operations) ->
            val message = JsonObject()
                .put("frameStartTime", frameStartTime)
                .put("frameEndTime", frameEndTime)
                .put("operations", JsonArray(operations))

            // Send to worker's frame processor
            vertx.eventBus().send("worker.$workerId.frame.manifest", message)

            log.debug("Sent {} operations to worker {} for frame starting at {}",
                operations.size, workerId, frameStartTime)
        }

        // Workers with no operations still need frame notification
        val workersWithOps = assignments.keys
        val activeWorkers = workerRegistry.getActiveWorkers()
        val workersWithoutOps = activeWorkers - workersWithOps

        workersWithoutOps.forEach { workerId ->
            val message = JsonObject()
                .put("frameStartTime", frameStartTime)
                .put("frameEndTime", frameEndTime)
                .put("operations", JsonArray())

            vertx.eventBus().send("worker.$workerId.frame.manifest", message)
        }
    }

    private fun broadcastFrameStart(frameStartTime: Long, frameEndTime: Long) {
        val frameStart = JsonObject()
            .put("frameStartTime", frameStartTime)
            .put("frameEndTime", frameEndTime)
            .put("frameDuration", frameConfig.frameDurationMs)

        vertx.eventBus().publish(ADDRESS_FRAME_START, frameStart)
    }

    private suspend fun waitForFrameCompletions(
        frameStartTime: Long,
        expectedWorkers: Set<String>,
        deadline: Long
    ) {
        // Clear previous frame completions
        frameCompletions.clear()

        // Wait until deadline
        while (System.currentTimeMillis() < deadline) {
            val completed = frameCompletions.values
                .filter { it.frameStartTime == frameStartTime }
                .map { it.workerId }
                .toSet()

            if (completed.containsAll(expectedWorkers)) {
                log.debug("All workers completed frame {}", frameStartTime)
                // Clean up assignment tracking
                frameAssignments.remove(frameStartTime)
                return
            }

            // Small delay to avoid busy waiting
            delay(10)
        }

        // Deadline reached - check who's missing
        val completed = frameCompletions.values
            .filter { it.frameStartTime == frameStartTime }
            .map { it.workerId }
            .toSet()
        val missing = expectedWorkers - completed

        if (missing.isNotEmpty()) {
            log.error("Frame {} incomplete. Missing workers: {}", frameStartTime, missing)
            pauseFrameProcessing("Workers failed to complete frame: $missing")

            // Attempt recovery
            launch {
                attemptFrameRecovery(frameStartTime, missing)
            }
        }
    }

    private suspend fun pauseFrameProcessing(reason: String) {
        log.warn("Pausing frame processing: {}", reason)
        isPaused = true

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
            // Mark missing workers as unhealthy
            workerRegistry.markWorkersUnhealthy(stillMissing)

            // Get incomplete operations from this frame
            val incompleteOps = frameAssignments[frameStartTime]
                ?.filterKeys { it in stillMissing }
                ?.values
                ?.flatten()
                ?: emptyList()

            if (incompleteOps.isNotEmpty()) {
                log.warn("Redistributing {} incomplete operations from frame {}",
                    incompleteOps.size, frameStartTime)

                // TODO: Redistribute to healthy workers in next frame
                // For now, log for manual intervention
            }
        }

        // Resume if we still have minimum workers
        if (workerRegistry.hasMinimumWorkers()) {
            resumeFrameProcessing()
        } else {
            log.error("Cannot resume - insufficient healthy workers")
        }
    }

    private suspend fun resumeFrameProcessing() {
        log.info("Resuming frame processing")
        isPaused = false

        vertx.eventBus().publish(ADDRESS_FRAME_RESUME, JsonObject()
            .put("timestamp", System.currentTimeMillis())
        )
    }

    override suspend fun stop() {
        // Cancel any pending frame timer
        nextFrameTimerId?.let {
            vertx.cancelTimer(it)
        }

        kafkaConsumer?.close()?.await()
        super.stop()
    }
}