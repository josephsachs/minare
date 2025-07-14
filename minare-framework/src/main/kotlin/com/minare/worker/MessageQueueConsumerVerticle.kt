package com.minare.worker

import com.minare.worker.upsocket.CommandMessageHandler
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kafka.client.consumer.KafkaConsumer
import io.vertx.kafka.client.consumer.KafkaConsumerRecord
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject
import kotlin.system.measureTimeMillis

/**
 * Production Kafka consumer verticle that reads Operations from Kafka
 * and delegates to CommandMessageHandler for processing.
 *
 * This consumer:
 * - Auto-commits offsets for simplicity (at-least-once delivery)
 * - Processes messages sequentially per partition (maintains order)
 * - Handles errors gracefully without stopping consumption
 */
class MessageQueueConsumerVerticle @Inject constructor(
    private val vertx: Vertx,
    private val commandMessageHandler: CommandMessageHandler
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(MessageQueueConsumerVerticle::class.java)

    companion object {
        private const val OPERATIONS_TOPIC = "minare.operations"
        private const val CONSUMER_GROUP = "minare-operation-processor"
    }

    // Configuration from environment
    private val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"
    private val consumerGroup = System.getenv("KAFKA_CONSUMER_GROUP") ?: CONSUMER_GROUP
    private val sessionTimeoutMs = System.getenv("KAFKA_SESSION_TIMEOUT_MS") ?: "30000"
    private val maxPollRecords = System.getenv("KAFKA_MAX_POLL_RECORDS") ?: "100"
    private val autoOffsetReset = System.getenv("KAFKA_AUTO_OFFSET_RESET") ?: "earliest"

    private var consumer: KafkaConsumer<String, String>? = null
    private var consumerDeploymentId: String? = null

    override suspend fun start() {
        log.info("Starting MessageQueueConsumerVerticle")
        log.info("Consumer configuration - bootstrap servers: {}, group: {}", bootstrapServers, consumerGroup)

        try {
            consumer = createConsumer()

            // Subscribe to the operations topic
            consumer!!.subscribe(OPERATIONS_TOPIC).await()
            log.info("Subscribed to topic: {}", OPERATIONS_TOPIC)

            // Start the polling loop
            startPolling()

            log.info("MessageQueueConsumerVerticle started successfully")

        } catch (e: Exception) {
            log.error("Failed to start MessageQueueConsumerVerticle", e)
            throw e
        }
    }

    private fun createConsumer(): KafkaConsumer<String, String> {
        val config = mutableMapOf<String, String>()

        // Connection
        config["bootstrap.servers"] = bootstrapServers
        config["group.id"] = consumerGroup

        // Deserialization
        config["key.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"
        config["value.deserializer"] = "org.apache.kafka.common.serialization.StringDeserializer"

        // Consumer behavior
        config["enable.auto.commit"] = "true"
        config["auto.commit.interval.ms"] = "5000"
        config["auto.offset.reset"] = autoOffsetReset
        config["session.timeout.ms"] = sessionTimeoutMs
        config["max.poll.records"] = maxPollRecords

        // Performance
        config["fetch.min.bytes"] = "1"
        config["fetch.max.wait.ms"] = "500"

        // Client identification
        config["client.id"] = "minare-consumer-${System.getenv("HOSTNAME") ?: "unknown"}"

        log.debug("Creating Kafka consumer with config: {}", config)

        return KafkaConsumer.create(vertx, config)
    }

    private fun startPolling() {
        consumer?.handler { record ->
            // Process each record in the Vert.x context
            launch(vertx.dispatcher()) {
                processRecord(record)
            }
        }

        consumer?.exceptionHandler { error ->
            log.error("Kafka consumer error", error)
            // Consumer will continue polling
        }

        consumer?.partitionsAssignedHandler { partitions ->
            log.info("Partitions assigned: {}", partitions.map { "${it.topic}:${it.partition}" })
        }

        consumer?.partitionsRevokedHandler { partitions ->
            log.info("Partitions revoked: {}", partitions.map { "${it.topic}:${it.partition}" })
        }
    }

    private suspend fun processRecord(record: KafkaConsumerRecord<String, String>) {
        val startTime = System.currentTimeMillis()

        try {
            log.debug("Processing message from partition {} offset {}",
                record.partition(), record.offset())

            // Parse the JsonArray of operations
            val operations = JsonArray(record.value())

            // Process each operation in the array
            for (i in 0 until operations.size()) {
                val operation = operations.getJsonObject(i)
                processOperation(operation)
            }

            val duration = System.currentTimeMillis() - startTime
            if (log.isDebugEnabled) {
                log.debug("Processed {} operations from partition {} offset {} in {}ms",
                    operations.size(), record.partition(), record.offset(), duration)
            }

        } catch (e: Exception) {
            log.error("Error processing record from partition {} offset {}: {}",
                record.partition(), record.offset(), record.value(), e)
            // Continue processing other messages
        }
    }

    /**
     * Process a single operation from the queue
     */
    private suspend fun processOperation(operationJson: JsonObject) {
        try {
            val action = operationJson.getString("action")
            val entityId = operationJson.getString("entity")
            val values = operationJson.getJsonObject("values") ?: JsonObject()
            val connectionId = values.getString("connectionId")

            if (connectionId == null) {
                log.warn("Operation missing connectionId: {}", operationJson)
                return
            }

            when (action) {
                "MUTATE" -> {
                    // Reconstruct the command format expected by CommandMessageHandler
                    val command = JsonObject()
                        .put("command", "mutate")
                        .put("entity", JsonObject()
                            .put("_id", entityId)
                            .put("type", values.getString("entityType"))
                            .put("version", operationJson.getLong("version"))
                            .put("state", values.copy().apply {
                                // Remove metadata fields that were added
                                remove("connectionId")
                                remove("entityType")
                            })
                        )

                    // Measure processing time
                    val processingTime = measureTimeMillis {
                        // Call CommandMessageHandler
                        commandMessageHandler.handle(connectionId, command)
                    }

                    if (log.isDebugEnabled) {
                        log.debug("Processed MUTATE for entity {} in {}ms", entityId, processingTime)
                    }
                }

                "CREATE" -> {
                    // TODO: Implement CREATE operation handling
                    log.warn("CREATE operation not yet implemented: {}", operationJson)
                }

                "DELETE" -> {
                    // TODO: Implement DELETE operation handling
                    log.warn("DELETE operation not yet implemented: {}", operationJson)
                }

                else -> {
                    log.warn("Unknown operation action: {}", action)
                }
            }
        } catch (e: Exception) {
            log.error("Error processing operation: {}", operationJson, e)
        }
    }

    override suspend fun stop() {
        log.info("Stopping MessageQueueConsumerVerticle")

        try {
            // Unsubscribe and close consumer
            consumer?.unsubscribe()?.await()
            consumer?.close()?.await()

            log.info("MessageQueueConsumerVerticle stopped successfully")
        } catch (e: Exception) {
            log.error("Error stopping MessageQueueConsumerVerticle", e)
        }
    }
}