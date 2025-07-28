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
 * Kafka consumer verticle that now routes operations through the frame processing system.
 *
 * In the new architecture:
 * - This consumes from Kafka and forwards to the coordinator
 * - The coordinator buffers operations by frame
 * - Workers process operations via FrameWorkerVerticle
 */
class MessageQueueConsumerVerticle @Inject constructor(
    private val vertx: Vertx
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(MessageQueueConsumerVerticle::class.java)

    companion object {
        private const val OPERATIONS_TOPIC = "minare.operations"
        private const val CONSUMER_GROUP = "minare-operation-processor"

        // Internal event bus address for operation processing
        const val ADDRESS_PROCESS_OPERATION = "worker.process.operation"
    }

    // Configuration from environment
    private val bootstrapServers = System.getenv("KAFKA_BOOTSTRAP_SERVERS") ?: "localhost:9092"
    private val consumerGroup = System.getenv("KAFKA_CONSUMER_GROUP") ?: CONSUMER_GROUP
    private val sessionTimeoutMs = System.getenv("KAFKA_SESSION_TIMEOUT_MS") ?: "30000"
    private val maxPollRecords = System.getenv("KAFKA_MAX_POLL_RECORDS") ?: "100"
    private val autoOffsetReset = System.getenv("KAFKA_AUTO_OFFSET_RESET") ?: "earliest"

    private var consumer: KafkaConsumer<String, String>? = null

    override suspend fun start() {
        log.info("Starting MessageQueueConsumerVerticle")

        // Only start consumer on worker instances
        val instanceRole = System.getenv("INSTANCE_ROLE")
        if (instanceRole != "WORKER") {
            log.info("Not starting Kafka consumer on non-worker instance (role: {})", instanceRole)
            return
        }

        setupOperationHandlers()

        log.info("MessageQueueConsumerVerticle started - operation handlers registered")
    }

    /**
     * Set up event bus handlers for operation processing.
     * These are called by FrameWorkerVerticle when processing manifests.
     */
    private fun setupOperationHandlers() {
        // Handler for MUTATE operations
        vertx.eventBus().consumer<JsonObject>("worker.process.MUTATE") { message ->
            launch {
                processOperation(message.body(), "MUTATE")
                message.reply(JsonObject().put("success", true))
            }
        }

        // Handler for CREATE operations
        vertx.eventBus().consumer<JsonObject>("worker.process.CREATE") { message ->
            launch {
                processOperation(message.body(), "CREATE")
                message.reply(JsonObject().put("success", true))
            }
        }

        // Handler for DELETE operations
        vertx.eventBus().consumer<JsonObject>("worker.process.DELETE") { message ->
            launch {
                processOperation(message.body(), "DELETE")
                message.reply(JsonObject().put("success", true))
            }
        }

        log.info("Registered operation handlers for MUTATE, CREATE, DELETE")
    }

    /**
     * Process a single operation from the frame manifest
     */
    /**
     * Process a single operation from the frame manifest
     */
    private suspend fun processOperation(operationJson: JsonObject, action: String) {
        try {
            val entityId = operationJson.getString("entityId")
            val operationId = operationJson.getString("id")
            val connectionId = operationJson.getString("connectionId")
            val entityType = operationJson.getString("entityType")

            log.debug("Processing {} operation {} for entity {}",
                action, operationId, entityId)

            when (action) {
                "MUTATE" -> {
                    // Reconstruct the command format expected by mutation handler
                    val command = JsonObject()
                        .put("command", "mutate")
                        .put("entity", JsonObject()
                            .put("_id", entityId)
                            .put("type", entityType)
                            .put("version", operationJson.getLong("version"))
                            .put("state", operationJson.getJsonObject("delta") ?: JsonObject())
                        )

                    // Send to MutationVerticle
                    val processingTime = measureTimeMillis {
                        val result = vertx.eventBus()
                            .request<JsonObject>("minare.mutation.process",
                                JsonObject()
                                    .put("connectionId", connectionId ?: "frame-processor")
                                    .put("entity", command.getJsonObject("entity"))
                            )
                            .await()

                        if (!result.body().getBoolean("success", false)) {
                            throw Exception(result.body().getString("error", "Mutation failed"))
                        }
                    }

                    log.debug("Processed MUTATE for entity {} in {}ms", entityId, processingTime)
                }

                "CREATE" -> {
                    // Send to appropriate handler when implemented
                    log.warn("CREATE operation not yet implemented: {}", operationJson)
                }

                "DELETE" -> {
                    // Send to appropriate handler when implemented
                    log.warn("DELETE operation not yet implemented: {}", operationJson)
                }

                else -> {
                    log.warn("Unknown operation action: {}", action)
                }
            }
        } catch (e: Exception) {
            log.error("Error processing operation: {}", operationJson, e)
            throw e
        }
    }

    override suspend fun stop() {
        log.info("Stopping MessageQueueConsumerVerticle")
        super.stop()
    }
}