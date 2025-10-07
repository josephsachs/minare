package com.minare.example.controller

import com.minare.controller.OperationController
import com.minare.core.operation.interfaces.MessageQueue
import com.minare.core.operation.models.Operation
import com.minare.core.operation.models.OperationType
import com.minare.example.ExampleEntityFactory
import com.minare.example.models.Node
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
    private var exampleEntityFactory: ExampleEntityFactory
) : OperationController() {
    private val log = LoggerFactory.getLogger(ExampleOperationController::class.java)

    /**
     * Convert incoming client commands to Operations before queueing to Kafka.
     *
     * @param message The raw message from the client
     * @return Operation, OperationSet, or null to skip processing
     */
    override suspend fun preQueue(message: JsonObject): Any? {
        val command = message.getString("command")
        val connectionId = message.getString("connectionId")

        return when (command) {
            "mutate" -> {
                // Convert mutate command to Operation
                val entityObject = message.getJsonObject("entity")
                if (entityObject == null) {
                    log.warn("Invalid mutate command: missing entity object")
                    return null
                }

                val entityId = entityObject.getString("_id")
                if (entityId == null) {
                    log.warn("Invalid mutate command: missing entity ID")
                    return null
                }

                val operation = Operation()
                    .entity(entityId)
                    .action(OperationType.MUTATE)
                    .delta(entityObject.getJsonObject("state") ?: JsonObject())

                /**
                 * NodeGraph only deals with Nodes, application must perform dispatch if more
                 * than one type. For more complicated entity schemes decomposition of preQueue
                 * into specific handler services is recommended.
                 */
                operation.entityType(Node::class)

                // Add version if present
                entityObject.getLong("version")?.let {
                    operation.version(it)
                }

                // Add connection context as metadata
                operation.meta(
                    JsonObject()
                        .put("connectionId", connectionId)
                        .toString()
                )

                log.debug("Created MUTATE operation for entity {} from connection {}", entityId, connectionId)

                operation
            }

            // Add other command types here as needed
            // "create" -> { ... }
            // "delete" -> { ... }

            else -> {
                // Unknown command - log and skip
                log.warn("Unknown command received: {} from connection: {}", command, connectionId)
                null
            }
        }
    }
}