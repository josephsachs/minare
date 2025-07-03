package com.minare.worker

import com.minare.operation.MessageQueue
import com.minare.worker.upsocket.CommandMessageHandler
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * Stub consumer verticle that reads Operations from Kafka and
 * calls CommandMessageHandler. This is a temporary implementation
 * that will be replaced by the frame-based consumer system.
 *
 * For now, this provides end-to-end testing capability for the
 * Kafka-based command flow.
 */
class MessageQueueConsumerVerticle @Inject constructor(
    private val messageQueue: MessageQueue,
    private val commandMessageHandler: CommandMessageHandler
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(MessageQueueConsumerVerticle::class.java)

    companion object {
        private const val OPERATIONS_TOPIC = "minare.operations"
        private const val POLL_INTERVAL_MS = 100L
    }

    override suspend fun start() {
        log.info("Starting MessageQueueConsumerVerticle")

        // Start polling for messages
        // In real implementation, this would use Kafka consumer API
        vertx.setPeriodic(POLL_INTERVAL_MS) { timerId ->
            launch(vertx.dispatcher()) {
                pollAndProcessMessages()
            }
        }
    }

    /**
     * Poll for messages from Kafka and process them.
     * This is a stub - real implementation would use Kafka consumer.
     */
    private suspend fun pollAndProcessMessages() {
        try {
            // Stub: In real implementation, poll from Kafka
            // For now, we'll use EventBus to simulate Kafka consumption

            // This would be replaced with:
            // val records = kafkaConsumer.poll(Duration.ofMillis(100))
            // records.forEach { record -> processOperation(record.value()) }

        } catch (e: Exception) {
            log.error("Error polling messages from queue", e)
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

                    // Call CommandMessageHandler
                    commandMessageHandler.handle(connectionId, command)
                }

                else -> {
                    log.warn("Unknown operation action: {}", action)
                }
            }
        } catch (e: Exception) {
            log.error("Error processing operation: {}", operationJson, e)
        }
    }

    /**
     * EventBus handler for testing - simulates Kafka consumption
     * This allows end-to-end testing before Kafka is fully integrated
     */
    fun registerTestHandler() {
        vertx.eventBus().consumer<JsonArray>("test.operations") { message ->
            launch(vertx.dispatcher()) {
                val operations = message.body()
                for (i in 0 until operations.size()) {
                    val operation = operations.getJsonObject(i)
                    processOperation(operation)
                }
            }
        }
    }

    override suspend fun stop() {
        log.info("Stopping MessageQueueConsumerVerticle")
    }
}