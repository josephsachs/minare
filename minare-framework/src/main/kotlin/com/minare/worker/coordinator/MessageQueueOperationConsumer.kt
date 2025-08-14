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
        try {
            // Check if backpressure is already active
            if (backpressureManager.isActive()) {
                // Commit this record without processing to avoid replay
                messageQueueConsumer?.commit()

                // Pause further consumption
                log.debug("Backpressure active - pausing consumption")
                messageQueueConsumer?.pause()
                return
            }

            val recordValue = record.value()

            val operations = try {
                val array = JsonArray(recordValue)
                // Convert JsonArray to list of JsonObjects
                array.mapNotNull {
                    when (it) {
                        is JsonObject -> it
                        else -> {
                            log.warn("Skipping non-object element in operation array: {}", it)
                            null
                        }
                    }
                }
            } catch (e: io.vertx.core.json.DecodeException) {
                // Not an array, try as single object for backward compatibility
                try {
                    listOf(JsonObject(recordValue))
                } catch (e2: Exception) {
                    log.error("Failed to parse Kafka record as either JsonArray or JsonObject: {}", recordValue, e2)
                    return
                }
            }

            // Process each operation in the batch sequentially
            var backpressureTriggered = false
            for (operation in operations) {
                if (!processOperation(operation)) {
                    // Backpressure was triggered, but continue to mark offset as processed
                    backpressureTriggered = true
                    break
                }
            }

            messageQueueConsumer?.commit()

            if (backpressureTriggered) {
                log.debug("Backpressure triggered during batch processing")
            }

        } catch (e: Exception) {
            log.error("Error processing Kafka record", e)
        }
    }

    /**
     * Process a single operation from the Kafka record.
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

        // Check buffer limits before processing
        val totalBuffered = coordinatorState.getTotalBufferedOperations()
        if (totalBuffered >= MAX_BUFFER_SIZE) {
            log.error("Operation buffer full ({}/{} operations), activating backpressure",
                totalBuffered, MAX_BUFFER_SIZE)

            // Activate backpressure
            backpressureManager.activate(
                frame = coordinatorState.frameInProgress,
                bufferedOps = totalBuffered,
                maxBuffer = MAX_BUFFER_SIZE
            )

            // Pause Kafka consumer
            messageQueueConsumer?.pause()

            // Broadcast backpressure activated event
            vertx.eventBus().publish(
                "minare.backpressure.activated",
                JsonObject()
                    .put("frameInProgress", coordinatorState.frameInProgress)
                    .put("bufferedOperations", totalBuffered)
                    .put("maxBufferSize", MAX_BUFFER_SIZE)
                    .put("timestamp", System.currentTimeMillis())
            )

            return false // Stop processing
        }

        if (totalBuffered >= BUFFER_WARNING_THRESHOLD) {
            log.warn("Operation buffer approaching limit: {}/{} operations",
                totalBuffered, MAX_BUFFER_SIZE)
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

        // Check if this is a late operation (before current frame)
        val frameInProgress = coordinatorState.frameInProgress
        if (logicalFrame < frameInProgress) {
            val framesLate = frameInProgress - logicalFrame

            log.warn("Late operation detected: operation targets frame {} but current frame is {} ({} frames late)",
                logicalFrame, frameInProgress, framesLate)

            lateOperationHandler.handleLateOperation(operation, logicalFrame, frameInProgress)
            return
        }

        // Check if operation is too far in the future
        if (coordinatorState.isPaused) {
            // During pause, enforce strict buffer limits
            if (!frameCalculator.isFrameWithinBufferLimit(logicalFrame, frameInProgress)) {
                log.error("Operation {} targets frame {} which exceeds pause buffer limit " +
                        "(current: {}, max allowed: {}). " +
                        "503 backpressure should be implemented here.",
                    operation.getString("id"), logicalFrame, frameInProgress,
                    frameInProgress + frameConfig.maxBufferFrames)
                // TODO: Implement actual 503 response mechanism
                return
            }
        }

        // Future frame - buffer normally
        coordinatorState.bufferOperation(operation, logicalFrame)

        if (log.isTraceEnabled) {
            log.trace("Buffered operation {} for logical frame {}",
                operation.getString("id"), logicalFrame)
        }

        // Trigger manifest preparation if this frame hasn't been prepared yet
        triggerManifestPreparation(logicalFrame)
    }

    /**
     * Trigger manifest preparation for a specific frame.
     * This is the key to event-driven manifest preparation.
     */
    private fun triggerManifestPreparation(logicalFrame: Long) {
        // Only trigger if the manifest hasn't been prepared yet
        if (logicalFrame > coordinatorState.lastPreparedManifest) {
            vertx.eventBus().send(FrameCoordinatorVerticle.ADDRESS_PREPARE_MANIFEST, JsonObject())

            if (log.isDebugEnabled) {
                log.debug("Triggered manifest preparation for frame {}", logicalFrame)
            }
        }
    }

    /**
     * Resume Kafka consumption after backpressure is cleared
     */
    fun resumeConsumption() {
        log.info("Resuming Kafka consumption after backpressure cleared")
        messageQueueConsumer?.resume()
    }

    /**
     * Get current consumer metrics for monitoring.
     * Enhanced with buffer status information.
     */
    fun getMetrics(): JsonObject {
        val bufferCounts = coordinatorState.getBufferedOperationCounts()
        val totalBuffered = coordinatorState.getTotalBufferedOperations()
        val bufferPercentage = (totalBuffered.toDouble() / MAX_BUFFER_SIZE * 100).toInt()

        return JsonObject()
            .put("connected", messageQueueConsumer != null)
            .put("topic", OPERATIONS_TOPIC)
            .put("paused", coordinatorState.isPaused)
            .put("sessionStart", coordinatorState.sessionStartTimestamp)
            .put("lastProcessedFrame", coordinatorState.lastProcessedFrame)
            .put("bufferedOperations", totalBuffered)
            .put("bufferCapacity", MAX_BUFFER_SIZE)
            .put("bufferPercentage", bufferPercentage)
            .put("frameBufferCounts", bufferCounts)
            .put("isApproachingLimit", coordinatorState.isApproachingBufferLimit())
    }
}