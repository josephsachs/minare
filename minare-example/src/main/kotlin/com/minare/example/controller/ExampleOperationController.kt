package com.minare.example.controller

import com.minare.controller.OperationController
import com.minare.operation.MessageQueue
import com.minare.operation.Operation
import com.minare.operation.OperationType
import com.minare.worker.upsocket.SyncCommandHandler
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Application-specific implementation of OperationController.
 * Handles conversion of client commands to Operations for Kafka.
 */
@Singleton
class ExampleOperationController @Inject constructor(
    messageQueue: MessageQueue,
    private val syncCommandHandler: SyncCommandHandler
) : OperationController(messageQueue) {

    private val log = LoggerFactory.getLogger(ExampleOperationController::class.java)

    /**
     * Convert incoming client commands to Operations before queueing to Kafka
     */
    override suspend fun preQueue(operations: JsonArray): JsonArray {
        // For now, we expect a single JsonObject (the raw message from client)
        // In the future, this might handle batches
        if (operations.size() != 1 || operations.getValue(0) !is JsonObject) {
            return operations // Pass through unchanged if not expected format
        }

        val message = operations.getJsonObject(0)
        val command = message.getString("command")
        val connectionId = message.getString("connectionId")

        when (command) {
            "mutate" -> {
                // Convert mutate command to Operation
                val entityObject = message.getJsonObject("entity")
                if (entityObject == null) {
                    log.warn("Invalid mutate command: missing entity object")
                    return JsonArray() // Return empty array to skip processing
                }

                val entityId = entityObject.getString("_id")
                if (entityId == null) {
                    log.warn("Invalid mutate command: missing entity ID")
                    return JsonArray() // Return empty array to skip processing
                }

                val operation = Operation()
                    .entity(entityId)
                    .action(OperationType.MUTATE)
                    .delta(entityObject.getJsonObject("state") ?: JsonObject())

                // Add version if present
                entityObject.getLong("version")?.let {
                    operation.version(it)
                }

                // Add connection context as metadata
                operation.value("connectionId", connectionId)
                operation.value("entityType", entityObject.getString("type"))

                // Return as JsonArray containing the built Operation
                return JsonArray().add(operation.build())
            }

            "sync" -> {
                // Sync commands bypass Kafka for now
                syncCommandHandler.handle(connectionId, message)
                return JsonArray() // Return empty array to skip Kafka
            }

            else -> {
                // Unknown command - log and skip
                log.warn("Unknown command received: {} from connection: {}", command, connectionId)
                return JsonArray() // Return empty array to skip processing
            }
        }
    }

    /**
     * Post-queue hook for any cleanup or additional processing
     * after messages have been sent to Kafka
     */
    override suspend fun postQueue(operations: JsonArray): JsonArray {
        // No post-processing needed for now
        return operations
    }
}