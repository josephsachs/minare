package com.minare.worker.coordinator

import com.minare.time.FrameConfiguration
import com.minare.time.FrameCalculator
import io.vertx.core.Vertx
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
    private val lateOperationHandler: LateOperationHandler
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
     */
    private fun handleKafkaRecord(record: io.vertx.kafka.client.consumer.KafkaConsumerRecord<String, String>) {
        try {
            val operation = JsonObject(record.value())
            val timestamp = operation.getLong("timestamp")

            if (timestamp == null) {
                log.error("Operation missing timestamp, cannot assign to frame: {}",
                    operation.encode())
                return
            }

            // Check buffer limits first
            val totalBuffered = coordinatorState.getTotalBufferedOperations()
            if (totalBuffered >= MAX_BUFFER_SIZE) {
                log.error("Operation buffer full ({}/{} operations), dropping operation",
                    totalBuffered, MAX_BUFFER_SIZE)
                // TODO: Implement proper backpressure/503 response
                return
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

            // Commit offset after successful processing
            messageQueueConsumer?.commit()

        } catch (e: Exception) {
            log.error("Error processing Kafka record", e)
        }
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
            vertx.eventBus().send(
                FrameCoordinatorVerticle.ADDRESS_PREPARE_MANIFEST,
                JsonObject().put("frame", logicalFrame)
            )

            if (log.isDebugEnabled) {
                log.debug("Triggered manifest preparation for frame {}", logicalFrame)
            }
        }
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