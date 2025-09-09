package com.minare.core.frames.coordinator.services

import com.minare.core.frames.coordinator.FrameCoordinatorState
import com.minare.core.frames.coordinator.handlers.LateOperationDecision
import com.minare.core.frames.coordinator.handlers.LateOperationHandler
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
    private val coordinatorState: FrameCoordinatorState,
    private val manifestBuilder: FrameManifestBuilder,
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

        messageQueueConsumer!!.subscribe(OPERATIONS_TOPIC).await()

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

            when (parsed) {
                is JsonObject -> {
                    handleOperation(parsed)
                }
                is JsonArray -> {
                    // Process batch of operations
                    // Will grow more complex as Entity rules governing operation atomicity are refined
                    // Consider refactoring outta here
                    for (i in 0 until parsed.size()) {
                        val operation = parsed.getJsonObject(i)
                        handleOperation(operation)
                    }
                }
            }

            // TODO: Ensure we commit only in the case of successfully completed operations
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
    private fun handleOperation(operation: JsonObject): Boolean {
        val timestamp = operation.getLong("timestamp") ?: run {
            log.error("Operation missing timestamp: {}", operation.encode())
            return false
        }

        val logicalFrame = coordinatorState.getLogicalFrame(timestamp)
        val frameInProgress = coordinatorState.frameInProgress

        if (logicalFrame <= frameInProgress) {
            val decision = lateOperationHandler.handleLateOperation(operation, logicalFrame, frameInProgress)

            when (decision) {
                is LateOperationDecision.Drop -> {
                    // Drop operation
                }
                is LateOperationDecision.Delay -> {
                    if (decision.targetFrame <= coordinatorState.lastPreparedManifest) {
                        manifestBuilder.assignToExistingManifest(operation, decision.targetFrame)
                    } else {
                        coordinatorState.bufferOperation(operation, decision.targetFrame)
                    }
                }
            }

            return true
        }

        if (logicalFrame <= coordinatorState.lastPreparedManifest) {
            manifestBuilder.assignToExistingManifest(operation, logicalFrame)
        } else {
            coordinatorState.bufferOperation(operation, logicalFrame)
        }

        return true
    }
}