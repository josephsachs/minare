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
    private val messageQueueOperationConsumer: MessageQueueOperationConsumer,
    private val frameManifestBuilder: FrameManifestBuilder,
    private val frameCompletionTracker: FrameCompletionTracker,
    private val frameRecoveryManager: FrameRecoveryManager,
    private val infraAddWorkerEvent: InfraAddWorkerEvent,
    private val infraRemoveWorkerEvent: InfraRemoveWorkerEvent,
    private val workerFrameCompleteEvent: WorkerFrameCompleteEvent,
    private val workerHeartbeatEvent: WorkerHeartbeatEvent
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameCoordinatorVerticle::class.java)

    companion object {
        const val ADDRESS_FRAME_START = "minare.coordinator.frame.start"
        const val ADDRESS_FRAME_PAUSE = "minare.coordinator.frame.pause"
        const val ADDRESS_FRAME_RESUME = "minare.coordinator.frame.resume"
        const val ADDRESS_FRAME_ALL_COMPLETE = "minare.coordinator.internal.frame-all-complete"

        // Configuration
        const val FRAME_STARTUP_GRACE_PERIOD = 5000L
    }

    override suspend fun start() {
        log.info("Starting FrameCoordinatorVerticle")
        vlog.setVerticle(this)

        setupEventBusConsumers()
        setupOperationConsumer()

        // Always start frame processing after grace period
        vertx.setTimer(FRAME_STARTUP_GRACE_PERIOD) {
            launch {
                log.info("Starting frame processing loop")
                startFirstFrame()
            }
        }
    }

    private fun setupEventBusConsumers() {
        // Infrastructure commands
        infraAddWorkerEvent.register()
        infraRemoveWorkerEvent.register()

        // Worker lifecycle
        launch {
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

    private suspend fun setupOperationConsumer() {
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

        messageQueueOperationConsumer.start(this)
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
            val healthCheckResult = requestHealthCheck(frameStartTime).await()

            if (!healthCheckResult.getBoolean("healthy")) {
                log.warn("Health check failed for frame {}: {}", frameStartTime, healthCheckResult.encode())
                pauseFrameProcessing("Health check failed: ${healthCheckResult.encode()}")
                return
            }

            val activeWorkers = workerRegistry.getActiveWorkers()

            // Log worker status but continue regardless
            if (activeWorkers.isEmpty()) {
                log.info("No active workers available for frame {}", frameStartTime)
            } else {
                log.info("Frame {} has {} active workers", frameStartTime, activeWorkers.size)
            }

            // 2. Get operations for this frame window (might be empty)
            val frameOperations = coordinatorState.extractFrameOperations(frameStartTime)

            // 3. Distribute operations (might result in empty manifests)
            val assignments = frameManifestBuilder.distributeOperations(frameOperations, activeWorkers)

            if (frameOperations.isEmpty() && activeWorkers.isEmpty()) {
                log.info("Empty frame {} with no workers, completing immediately", frameStartTime)
                onFrameComplete(frameStartTime)
                return
            }

            // 4. Always write manifests (empty or not)
            frameManifestBuilder.writeManifestsToMap(frameStartTime, frameEndTime, assignments, activeWorkers)

            // 5. Broadcast frame timing to all workers
            broadcastFrameStart(frameStartTime, frameEndTime)

            // 6. Start deadline monitor
            startFrameDeadlineMonitor(frameStartTime, frameEndTime)

        } catch (e: Exception) {
            log.error("Error executing frame {}", frameStartTime, e)
            pauseFrameProcessing("Frame execution error: ${e.message}")
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
        val missing = frameCompletionTracker.getMissingWorkers(frameStartTime)

        if (missing.isNotEmpty()) {
            log.warn("Frame {} progress check: {} workers incomplete at 80% mark",
                frameStartTime, missing.size)
        }
    }

    private suspend fun onFrameComplete(frameStartTime: Long) {
        log.info("Frame {} completed successfully", frameStartTime)

        // Clear distributed maps for this frame
        clearFrameMaps(frameStartTime)

        // Calculate next frame time
        val nextFrameStart = frameStartTime +
                frameConfig.frameDurationMs + frameConfig.frameOffsetMs
        coordinatorState.currentFrameStart = nextFrameStart

        // Wait until it's time for the next frame
        val now = System.currentTimeMillis()
        val waitTime = nextFrameStart - now

        if (waitTime > 0) {
            log.debug("Waiting {}ms until next frame", waitTime)
            delay(waitTime)
        } else if (waitTime < -frameConfig.frameOffsetMs) {
            log.warn("Frame timing drift detected: {}ms behind schedule", -waitTime)
        }

        // Now execute next frame
        executeFrame()
    }

    private fun clearFrameMaps(frameStartTime: Long) {
        frameManifestBuilder.clearFrameManifests(frameStartTime)
        frameCompletionTracker.clearFrameCompletions(frameStartTime)
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
        val recoveryResult = frameRecoveryManager.attemptFrameRecovery(frameStartTime, missingWorkers)

        if (!recoveryResult.success && recoveryResult.remainingMissing.isNotEmpty()) {
            // Check for incomplete operations from failed workers
            val incompleteOps = getIncompleteOperations(frameStartTime, recoveryResult.remainingMissing.toList())

            if (incompleteOps.isNotEmpty()) {
                log.warn("Found {} incomplete operations from failed workers", incompleteOps.size)
                // Could redistribute to healthy workers in future
            }
        }

        // Proceed based on recommendation
        when (recoveryResult.recommendation) {
            RecoveryRecommendation.CONTINUE -> {
                onFrameComplete(frameStartTime)
            }
            RecoveryRecommendation.CONTINUE_DEGRADED -> {
                log.warn("Continuing with degraded capacity")
                onFrameComplete(frameStartTime)
            }
            RecoveryRecommendation.PAUSE_AND_WAIT -> {
                frameRecoveryManager.pauseFrameProcessing(
                    "Too many worker failures: ${recoveryResult.remainingMissing.size} workers still missing"
                )
            }
            RecoveryRecommendation.RETRY_RECOVERY -> {
                // Could implement retry logic here
                onFrameComplete(frameStartTime)
            }
        }
    }

    private fun getIncompleteOperations(
        frameStartTime: Long,
        failedWorkers: List<String>
    ): List<JsonObject> {
        val incompleteOps = mutableListOf<JsonObject>()

        failedWorkers.forEach { workerId ->
            val manifest = frameManifestBuilder.getManifest(frameStartTime, workerId)

            manifest?.getJsonArray("operations")?.forEach { op ->
                if (op is JsonObject) {
                    val operationId = op.getString("id")

                    if (!frameCompletionTracker.isOperationCompleted(frameStartTime, operationId)) {
                        incompleteOps.add(op)
                    }
                }
            }
        }

        return incompleteOps
    }

    /**
     * Request health check and wait for result.
     */
    private fun requestHealthCheck(frameStartTime: Long): io.vertx.core.Future<JsonObject> {
        val promise = io.vertx.core.Promise.promise<JsonObject>()

        // Calculate grace period from instance frameConfig
        val healthCheckGracePeriod = (frameConfig.frameOffsetMs * frameConfig.coordinationWaitPeriod).toLong()

        // Set up one-time consumer for the response
        val consumer = vertx.eventBus().consumer<JsonObject>(
            FrameWorkerHealthMonitorVerticle.ADDRESS_HEALTH_CHECK_RESULT
        ) { message ->
            val result = message.body()
            if (result.getLong("frameStartTime") == frameStartTime) {
                promise.complete(result)
            }
        }

        // Request health check
        vertx.eventBus().send(
            FrameWorkerHealthMonitorVerticle.ADDRESS_HEALTH_CHECK_REQUEST,
            JsonObject().put("frameStartTime", frameStartTime)
        )

        // Timeout after grace period
        vertx.setTimer(healthCheckGracePeriod) {
            consumer.unregister()
            if (!promise.future().isComplete) {
                promise.fail("Health check timeout")
            }
        }

        return promise.future()
    }

    override suspend fun stop() {
        log.info("Stopping FrameCoordinatorVerticle")
        messageQueueOperationConsumer.stop()
        super.stop()
    }
}