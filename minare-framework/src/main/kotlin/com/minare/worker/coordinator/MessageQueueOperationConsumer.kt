package com.minare.worker.coordinator

import com.minare.time.FrameConfiguration
import com.minare.utils.EventBusUtils
import io.vertx.core.Vertx
import io.vertx.core.eventbus.EventBus
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
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
        config["bootstrap.servers"] = bootstrapServers
        config["group.id"] = "minare-coordinator"
        config["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        config["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        config["enable.auto.commit"] = "true"
        config["auto.commit.interval.ms"] = "1000"
        config["auto.offset.reset"] = "latest"

        return config
    }

    /**
     * Handle a single Kafka record containing operations.
     */
    private fun handleKafkaRecord(record: io.vertx.kafka.client.consumer.KafkaConsumerRecord<String, String>) {
        try {
            val operations = JsonArray(record.value())

            operations.forEach { op ->
                if (op is JsonObject) {
                    bufferOperation(op)
                }
            }
        } catch (e: Exception) {
            log.error("Error processing Kafka record", e)
        }
    }

    /**
     * Buffer a single operation into the appropriate frame.
     * Now implements backpressure checking and triggers manifest preparation.
     */
    private fun bufferOperation(operation: JsonObject) {
        val timestamp = operation.getLong("timestamp")
            ?: throw IllegalArgumentException("Operation missing timestamp")

        // Check buffer capacity before processing
        val currentBufferSize = coordinatorState.getTotalBufferedOperations()

        if (currentBufferSize >= MAX_BUFFER_SIZE) {
            log.error("Operation buffer full ({} operations), dropping operation {}. " +
                    "503 backpressure should be implemented here.",
                currentBufferSize, operation.getString("id"))
            // TODO: Implement actual 503 response mechanism to Kafka producers
            return
        } else if (currentBufferSize >= BUFFER_WARNING_THRESHOLD) {
            log.warn("Operation buffer approaching limit: {} / {} operations",
                currentBufferSize, MAX_BUFFER_SIZE)
        }

        // If we don't have a session start yet, we're not ready to process
        val sessionStart = coordinatorState.sessionStartTimestamp
        if (sessionStart == 0L) {
            log.warn("No session start timestamp set, dropping operation {}",
                operation.getString("id"))
            return
        }

        // Calculate logical frame based on session start
        val relativeTimestamp = timestamp - sessionStart
        val logicalFrame = if (relativeTimestamp < 0) {
            -1L // Before session start
        } else {
            relativeTimestamp / frameConfig.frameDurationMs
        }

        val lastProcessedFrame = coordinatorState.lastProcessedFrame
        val frameInProgress = coordinatorState.frameInProgress

        if (logicalFrame < 0 || logicalFrame <= lastProcessedFrame) {
            // Late operation - use configured handler
            if (log.isDebugEnabled) {
                log.debug("Late operation detected - op timestamp: {}, logical frame: {}, last processed: {}",
                    timestamp, logicalFrame, lastProcessedFrame)
            }

            val decision = lateOperationHandler.handleLateOperation(
                operation,
                logicalFrame,
                frameInProgress
            )

            when (decision) {
                is LateOperationDecision.Drop -> {
                    // Already logged by handler
                }
                is LateOperationDecision.Delay -> {
                    // Add to the target frame
                    coordinatorState.bufferOperation(operation, decision.targetFrame)
                    log.debug("Delayed operation {} to frame {}",
                        operation.getString("id"), decision.targetFrame)

                    // Trigger manifest preparation for the delayed frame
                    triggerManifestPreparation(decision.targetFrame)
                }
            }
            return
        }

        // Check if operation is too far in the future (during pause)
        if (coordinatorState.isPaused) {
            val maxAllowedFrame = frameInProgress + FrameCoordinatorVerticle.MAX_BUFFER_FRAMES
            if (logicalFrame > maxAllowedFrame) {
                log.warn("Operation {} for frame {} exceeds buffer limit during pause (current frame: {}, max: {}). " +
                        "503 backpressure should be implemented here.",
                    operation.getString("id"), logicalFrame, frameInProgress, maxAllowedFrame)
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