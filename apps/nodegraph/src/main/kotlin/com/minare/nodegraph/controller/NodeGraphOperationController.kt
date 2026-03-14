package com.minare.nodegraph.controller

import com.minare.controller.OperationController
import com.minare.core.entity.models.Entity
import com.minare.core.operation.models.FailurePolicy
import com.minare.core.operation.models.Operation
import com.minare.core.operation.models.OperationSet
import com.minare.core.operation.models.OperationType
import com.minare.nodegraph.models.Node
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Application-specific implementation of OperationController.
 * Handles conversion of client commands to Operations for Kafka.
 */
@Singleton
class NodeGraphOperationController @Inject constructor(
    private val channelController: NodeGraphChannelController
) : OperationController() {
    private val log = LoggerFactory.getLogger(NodeGraphOperationController::class.java)

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

                val state = entityObject.getJsonObject("state") ?: JsonObject()
                operation.entityType(Node::class)
                entityObject.getLong("version")?.let { operation.version(it) }

                state.put("lastOperation", operation.build()) // A record of what we just did
                                                              // that will be included in the update

                operation.delta(state)

                log.debug("Created MUTATE operation ${operation.id}")

                operation
            }

            "create" -> {
                val entityObject = message.getJsonObject("entity")
                if (entityObject == null) {
                    log.warn("Invalid create command: missing entity object")
                    return null
                }

                val operation = Operation()
                    .entityType(Node::class)
                    .action(OperationType.CREATE)
                    .delta(entityObject.getJsonObject("state") ?: JsonObject())
                    .meta(JsonObject().put("connectionId", connectionId).encode())

                log.debug("Created CREATE operation from connection {}", connectionId)
                operation
            }

            "delete" -> {
                val entityObject = message.getJsonObject("entity")
                if (entityObject == null) {
                    log.warn("Invalid delete command: missing entity object")
                    return null
                }

                val entityId = entityObject.getString("_id")
                if (entityId == null) {
                    log.warn("Invalid delete command: missing entity ID")
                    return null
                }

                val operation = Operation()
                    .entity(entityId)
                    .entityType(Node::class)
                    .action(OperationType.DELETE)
                    .delta(JsonObject())
                    .meta(JsonObject().put("connectionId", connectionId).encode())

                log.debug("Created DELETE operation for entity {} from connection {}", entityId, connectionId)
                operation
            }

            "operationSet" -> {
                val failurePolicyStr = message.getString("failurePolicy", "ABORT")
                val steps = message.getJsonArray("steps")

                if (steps == null || steps.isEmpty) {
                    log.warn("Invalid operationSet command: missing or empty steps")
                    return null
                }

                val failurePolicy = runCatching { FailurePolicy.valueOf(failurePolicyStr) }
                    .getOrElse {
                        log.warn("Unknown failurePolicy '{}', defaulting to ABORT", failurePolicyStr)
                        FailurePolicy.ABORT
                    }

                val set = OperationSet().failurePolicy(failurePolicy)

                for (i in 0 until steps.size()) {
                    val step = steps.getJsonObject(i)
                    val entityId = step.getString("entityId")

                    when (step.getString("action")) {
                        "MUTATE" -> set.add(
                            Operation()
                                .entity(entityId ?: continue)
                                .entityType(Node::class)
                                .action(OperationType.MUTATE)
                                .delta(step.getJsonObject("delta") ?: JsonObject())
                        )
                        "CREATE" -> set.add(
                            Operation()
                                .entityType(Node::class)
                                .action(OperationType.CREATE)
                                .delta(step.getJsonObject("delta") ?: JsonObject())
                        )
                        "DELETE" -> set.add(
                            Operation()
                                .entity(entityId ?: continue)
                                .entityType(Node::class)
                                .action(OperationType.DELETE)
                                .delta(JsonObject())
                        )
                        else -> log.warn("Unknown step action in operationSet: {}", step.getString("action"))
                    }
                }

                if (set.isEmpty()) {
                    log.warn("operationSet command produced an empty set — skipping")
                    return null
                }

                log.debug("Created operationSet with {} steps, failurePolicy={}", steps.size(), failurePolicy)
                set
            }

            else -> {
                log.warn("Unknown command received: {} from connection: {}", command, connectionId)
                null
            }
        }
    }

    override suspend fun afterCreateOperation(operation: JsonObject, entity: Entity) {
        val defaultChannel = channelController.getDefaultChannel()
            ?: throw Exception("No default channel configured")
        channelController.addEntity(entity, defaultChannel)
    }

    override suspend fun afterDeleteOperation(operation: JsonObject, entity: Entity) {
        val defaultChannel = channelController.getDefaultChannel()
            ?: throw Exception("No default channel configured")
        channelController.removeEntity(entity, defaultChannel)
    }
}