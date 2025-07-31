package com.minare.worker.coordinator

import com.minare.time.FrameConfiguration
import io.vertx.core.Vertx
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
 */
@Singleton
class MessageQueueOperationConsumer @Inject constructor(
    private val vertx: Vertx,
    private val frameConfig: FrameConfiguration,
    private val coordinatorState: FrameCoordinatorState
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
        setupmessageQueueConsumer()
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
    private suspend fun setupmessageQueueConsumer() {
        val config = createKafkaConfig()

        messageQueueConsumer = KafkaConsumer.create<String, String>(vertx, config)

        // Subscribe to operations topic
        messageQueueConsumer!!.subscribe(OPERATIONS_TOPIC).await()

        // Start consuming
        messageQueueConsumer!!.handler { record ->
            if (!coordinatorState.isPaused) {
                handleKafkaRecord(record)
            }
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
     * This is where we'll implement the logical frame assignment.
     */
    private fun bufferOperation(operation: JsonObject) {
        val timestamp = operation.getLong("timestamp") ?: System.currentTimeMillis()

        // Calculate the logical frame for this operation
        val calculatedFrame = coordinatorState.getFrameStartTime(timestamp)

        // FIXME: This is the bug! We need to check if the frame has already been processed
        // For now, keeping the existing logic to maintain compatibility
        val frameStart = if (calculatedFrame < coordinatorState.currentFrameStart) {
            coordinatorState.currentFrameStart
        } else {
            calculatedFrame
        }

        coordinatorState.bufferOperation(operation, frameStart)

        if (log.isTraceEnabled) {
            log.trace("Buffered operation {} for frame {}",
                operation.getString("id"), frameStart)
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
    }
}