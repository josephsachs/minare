package com.minare.worker

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
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
class WorkerOperationHandlerVerticle @Inject constructor(
    private val vertx: Vertx
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(WorkerOperationHandlerVerticle::class.java)

    override suspend fun start() {
        log.info("Starting WorkerOperationHandlerVerticle")

        setupOperationHandlers()

        log.info("WorkerOperationHandlerVerticle started - operation handlers registered")
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
        log.info("Stopping WorkerOperationHandlerVerticle")
        super.stop()
    }
}