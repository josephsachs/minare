package com.minare.worker.coordinator

import com.minare.time.FrameConfiguration
import com.minare.time.FrameCalculator
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.CoroutineScope
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Handles Kafka consumption for the frame coordinator.
 * Responsible for setting up the Kafka consumer and buffering operations
 * into the appropriate logical frames.
 *
 * Updated with event-driven manifest preparation and backpressure handling.
 */
@Singleton
class MessageQueueOperationConsumer @Inject constructor(
    private val vertx: Vertx,
    private val frameConfig: FrameConfiguration,
    private val frameCalculator: FrameCalculator,
    private val coordinatorState: FrameCoordinatorState,
    private val lateOperationHandler: LateOperationHandler,
    private val backpressureManager: BackpressureManager
) {
    private val log = LoggerFactory.getLogger(MessageQueueOperationConsumer::class.java)

    private var messageQueueConsumer: KafkaConsumer<String, String>? = null
    private var consumerScope: CoroutineScope? = null

    companion object {
        const val OPERATIONS_TOPIC = "minare.operations"

        // Buffer limits - operations count, not frame count
        const val MAX_BUFFER_SIZE = 10000  // Maximum operations to buffer
        const val BUFFER_WARNING_THRESHOLD = 8000  // Warn when approaching limit
    }

    /**
     * Initialize and start the Kafka consumer.
     * Should be called when the coordinator starts.
     */
    suspend fun start(scope: CoroutineScope) {
        consumerScope = scope
        setupMessageQueueConsumer()
    }

    /**
     * Stop the Kafka consumer gracefully.
     * Should be called when the coordinator stops.
     */
    suspend fun stop() {
        messageQueueConsumer?.close()?.await()
        messageQueueConsumer = null
        log.info("Kafka consumer stopped")
    }

    /**
     * Set up the Kafka consumer with appropriate configuration.
     */
    private suspend fun setupMessageQueueConsumer() {
        val config = createKafkaConfig()

        // TEMPORARY DEBUG
        log.warn("DEBUG: Kafka consumer config - bootstrap: {}, topic: {}, group: {}",
            config["bootstrap.servers"], OPERATIONS_TOPIC, config["group.id"])

        messageQueueConsumer = KafkaConsumer.create<String, String>(vertx, config)

        // Subscribe to operations topic
        messageQueueConsumer!!.subscribe(OPERATIONS_TOPIC).await()

        // Start consuming
        messageQueueConsumer!!.handler { record ->
            handleKafkaRecord(record)
        }

        log.info("Kafka consumer started, subscribed to {}", OPERATIONS_TOPIC)
    }

    /**
     * Create Kafka consumer configuration.
     * Extracted for easier testing and configuration management.
     */
    private fun createKafkaConfig(): Map<String, String> {
        val config = mutableMapOf<String, String>()

        val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"
        val groupId = System.getenv("KAFKA_GROUP_ID") ?: "minare-coordinator"

        config["bootstrap.servers"] = bootstrapServers
        config["group.id"] = groupId
        config["auto.offset.reset"] = "earliest"
        config["enable.auto.commit"] = "false"
        config["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        config["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"

        return config
    }


    /**
     * Handle a single Kafka record by buffering it to the appropriate frame.
     * This is the main integration point between Kafka and frame processing.
     *
     * Updated to handle both single operations (JsonObject) and batched operations (JsonArray).
     */
    private fun handleKafkaRecord(record: io.vertx.kafka.client.consumer.KafkaConsumerRecord<String, String>) {

        // TEMPORARY DEBUG
        log.warn("DEBUG: Kafka record received - topic: {}, partition: {}, offset: {}, value length: {}",
            record.topic(), record.partition(), record.offset(), record.value()?.length ?: 0)

        try {
            val value = record.value()
            if (value.isNullOrEmpty()) {
                log.warn("Received empty Kafka message")
                return
            }

            // Try parsing as JsonObject first (single operation)
            val parsed = try {
                JsonObject(value)
            } catch (e: Exception) {
                // If that fails, try JsonArray (batch of operations)
                try {
                    JsonArray(value)
                } catch (e2: Exception) {
                    log.error("Failed to parse Kafka message as JSON: {}", value)
                    return
                }
            }

            // Process based on type
            when (parsed) {
                is JsonObject -> {
                    if (!processOperation(parsed)) {
                        // Backpressure activated, stop processing
                        return
                    }
                }
                is JsonArray -> {
                    // Process batch of operations
                    for (i in 0 until parsed.size()) {
                        val operation = parsed.getJsonObject(i)
                        if (!processOperation(operation)) {
                            // Backpressure activated, stop processing
                            return
                        }
                    }
                }
            }

            // Commit after successful processing
            messageQueueConsumer?.commit()

        } catch (e: Exception) {
            log.error("Error processing Kafka record", e)
        }
    }

    /**
     * Resume consumption after backpressure was activated.
     * Called by the coordinator when buffer space is available.
     */
    fun resumeConsumption() {
        messageQueueConsumer?.resume()
        log.info("Resumed Kafka consumption after backpressure release")
    }

    /**
     * Process a single operation, checking buffer limits and routing to frames.
     * Updated to properly enforce frame-based buffer limits.
     *
     * Note: This method does not control Kafka commits. We always commit to avoid
     * duplicate operations. Atomicity and idempotency must be handled at the operation level.
     *
     * @return true if processing should continue, false if backpressure was activated
     */
    private fun processOperation(operation: JsonObject): Boolean {
        val timestamp = operation.getLong("timestamp")

        if (timestamp == null) {
            log.error("Operation missing timestamp, cannot assign to frame: {}",
                operation.encode())
            return true // Continue processing other operations
        }

        // Check FRAME buffer limits before processing
        val bufferedFrameCount = coordinatorState.getBufferedFrameCount()

        if (bufferedFrameCount >= frameConfig.maxBufferFrames) {
            val totalBuffered = coordinatorState.getTotalBufferedOperations()
            log.error("Frame buffer limit exceeded: {} frames buffered (max: {}), " +
                    "containing {} total operations. Activating backpressure",
                bufferedFrameCount, frameConfig.maxBufferFrames, totalBuffered)

            // Activate backpressure
            backpressureManager.activate(
                frame = coordinatorState.frameInProgress,
                bufferedOps = totalBuffered,
                maxBuffer = frameConfig.maxBufferFrames
            )

            // Pause Kafka consumer
            messageQueueConsumer?.pause()

            // Broadcast backpressure activated event
            vertx.eventBus().publish(
                "minare.backpressure.activated",
                JsonObject()
                    .put("frameInProgress", coordinatorState.frameInProgress)
                    .put("bufferedFrames", bufferedFrameCount)
                    .put("maxBufferFrames", frameConfig.maxBufferFrames)
                    .put("bufferedOperations", totalBuffered)
                    .put("timestamp", System.currentTimeMillis())
            )

            return false // Stop processing
        }

        // Warn when approaching frame buffer limit
        if (bufferedFrameCount >= frameCalculator.getBufferWarningThreshold()) {
            log.warn("Frame buffer approaching limit: {} frames buffered (max: {})",
                bufferedFrameCount, frameConfig.maxBufferFrames)
        }

        // Route to appropriate handler based on session state
        if (coordinatorState.sessionStartTimestamp == 0L) {
            handlePreSessionOperation(operation)
        } else {
            handleSessionOperation(operation, timestamp)
        }

        return true // Continue processing
    }

    /**
     * Handle operations that arrive before a session starts.
     * These are buffered as "pending" operations.
     */
    private fun handlePreSessionOperation(operation: JsonObject) {
        coordinatorState.bufferPendingOperation(operation)

        if (log.isDebugEnabled) {
            log.debug("Buffered pre-session operation {} (total pending: {})",
                operation.getString("id"), coordinatorState.getPendingOperationCount())
        }
    }

    /**
     * Handle operations during an active session.
     * Routes to appropriate frame based on timestamp.
     */
    private fun handleSessionOperation(operation: JsonObject, timestamp: Long) {
        val logicalFrame = coordinatorState.getLogicalFrame(timestamp)

        // TEMPORARY DEBUG
        log.warn("DEBUG: Processing operation {} with timestamp {} -> logical frame {}",
            operation.getString("id"), timestamp, logicalFrame)

        // Check if this is a late operation (before current frame)
        val frameInProgress = coordinatorState.frameInProgress

        // TEMPORARY DEBUG
        log.warn("DEBUG: frameInProgress = {}, logicalFrame = {}", frameInProgress, logicalFrame)

        if (logicalFrame < frameInProgress) {
            val framesLate = frameInProgress - logicalFrame

            log.warn("Late operation detected: operation targets frame {} but current frame is {} ({} frames late)",
                logicalFrame, frameInProgress, framesLate)

            val decision = lateOperationHandler.handleLateOperation(operation, logicalFrame, frameInProgress)

            when (decision) {
                is LateOperationDecision.Drop -> return
                is LateOperationDecision.Delay -> {
                    coordinatorState.bufferOperation(operation, decision.targetFrame)
                    // Let the normal scheduling handle manifest preparation
                    return  // ADD THIS RETURN
                }
            }
        }

        // Check if operation is too far in the future
        if (coordinatorState.isPaused) {
            // During pause, enforce strict buffer limits
            if (!frameCalculator.isFrameWithinBufferLimit(logicalFrame, frameInProgress)) {
                log.error("Operation {} targets frame {} which exceeds pause buffer limit " +
                        "(current: {}, max allowed: {}). " +
                        "503 backpressure should be implemented here.",
                    operation.getString("id"), logicalFrame,
                    frameInProgress, frameInProgress + frameConfig.maxBufferFrames)

                // Note: We should return 503 to HTTP clients here, but since this is
                // Kafka consumption, we handle it via backpressure activation above
                return
            }
        }

        // Buffer the operation to its target frame
        coordinatorState.bufferOperation(operation, logicalFrame)

        // TEMPORARY DEBUG
        log.warn("DEBUG: Buffered operation {} to frame {}", operation.getString("id"), logicalFrame)

        // Trigger manifest preparation if needed
        if (shouldTriggerManifestPreparation(logicalFrame)) {
            triggerManifestPreparation(logicalFrame)
        }
    }

    /**
     * Determine if we should trigger manifest preparation for a frame.
     * This provides event-driven manifest preparation when operations arrive.
     */
    private fun shouldTriggerManifestPreparation(logicalFrame: Long): Boolean {
        // Already prepared?
        if (logicalFrame <= coordinatorState.lastPreparedManifest) {
            return false
        }

        // During pause, respect buffer limits
        if (coordinatorState.isPaused) {
            val frameInProgress = coordinatorState.frameInProgress
            return frameCalculator.isFrameWithinBufferLimit(logicalFrame, frameInProgress)
        }

        // Normal operation - prepare if within lookahead window
        val currentFrame = frameCalculator.getCurrentLogicalFrame(coordinatorState.sessionStartNanos)
        return logicalFrame <= currentFrame + frameConfig.normalOperationLookahead
    }

    /**
     * Trigger manifest preparation for a specific frame.
     * Sends event to coordinator to prepare the manifest.
     */
    private fun triggerManifestPreparation(logicalFrame: Long) {
        vertx.eventBus().send(
            FrameCoordinatorVerticle.ADDRESS_PREPARE_MANIFEST,
            JsonObject()
                .put("logicalFrame", logicalFrame)
                .put("trigger", "operation_arrival")
        )
    }

    /**
     * Get current metrics for monitoring
     */
    fun getMetrics(): JsonObject {
        val isActive = messageQueueConsumer != null

        return JsonObject()
            .put("topic", OPERATIONS_TOPIC)
            .put("consumerActive", isActive)
            .put("bufferWarningThreshold", BUFFER_WARNING_THRESHOLD)
            .put("maxBufferSize", MAX_BUFFER_SIZE)
    }
}