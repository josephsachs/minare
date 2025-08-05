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
 * Updated to support logical frames and late operation handling.
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
     * Now implements logical frame assignment with late operation detection.
     */
    private fun bufferOperation(operation: JsonObject) {
        val timestamp = operation.getLong("timestamp")
            ?: throw IllegalArgumentException("Operation missing timestamp")

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
                }
                //is LateOperationDecision.Replay -> {
                //    // Trigger replay - would need to emit an event
                //    log.error("Replay requested from frame {} - not yet implemented",
                //        decision.fromFrame)
                //    // TODO: Emit replay event
                //}
            }
            return
        }

        // Future frame - buffer normally
        coordinatorState.bufferOperation(operation, logicalFrame)

        if (log.isTraceEnabled) {
            log.trace("Buffered operation {} for logical frame {}",
                operation.getString("id"), logicalFrame)
        }
    }

    /**
     * Get current consumer metrics for monitoring.
     */
    fun getMetrics(): JsonObject {
        return JsonObject()
            .put("connected", messageQueueConsumer != null)
            .put("topic", OPERATIONS_TOPIC)
            .put("paused", coordinatorState.isPaused)
            .put("sessionStart", coordinatorState.sessionStartTimestamp)
            .put("lastProcessedFrame", coordinatorState.lastProcessedFrame)
    }
}