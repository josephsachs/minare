package com.minare.integration.controller

import com.google.inject.Inject
import com.minare.controller.OperationController
import com.minare.core.entity.models.Entity
import com.minare.core.operation.models.Assert
import com.minare.core.operation.models.FailurePolicy
import com.minare.core.operation.models.FunctionCall
import com.minare.core.operation.models.Operation
import com.minare.core.operation.models.OperationSet
import com.minare.core.operation.models.OperationType
import com.minare.core.operation.models.Trigger
import com.minare.integration.models.Node
import com.minare.integration.models.SetTestEntity
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Singleton

/**
 * Application-specific implementation of OperationController.
 * Handles conversion of client commands to Operations for Kafka.
 */
@Singleton
class TestOperationController @Inject constructor(
    private val channelController: TestChannelController
) : OperationController() {
    private val log = LoggerFactory.getLogger(TestOperationController::class.java)

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
            "create" -> {
                val entityObject = message.getJsonObject("entity")
                if (entityObject == null) {
                    log.warn("Invalid create command: missing entity object")
                    return null
                }

                val entityType = entityObject.getString("type")
                if (entityType == null) {
                    log.warn("Invalid create command: missing entity type")
                    return null
                }

                val operation = Operation()
                    .entityType(Node::class)  // DEFERRED: Should dispatch by type
                    .action(OperationType.CREATE)
                    .delta(entityObject.getJsonObject("state") ?: JsonObject())
                    .meta(JsonObject().put("connectionId", connectionId).encode())

                log.debug("Created CREATE operation for new {} from connection {}", entityType, connectionId)
                operation
            }

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
                    .delta(entityObject.getJsonObject("state") ?: JsonObject())
                    .entityType(Node::class)  // DEFERRED: Should dispatch by type

                entityObject.getLong("version")?.let { operation.version(it) }
                operation.meta(JsonObject().put("connectionId", connectionId).encode())

                log.debug("Created MUTATE operation for entity {} from connection {}", entityId, connectionId)
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
                    .entityType(Node::class)  // DEFERRED: Should dispatch by type or lookup
                    .action(OperationType.DELETE)
                    .delta(JsonObject())  // Empty delta for delete
                    .meta(JsonObject().put("connectionId", connectionId).encode())

                log.debug("Created DELETE operation for entity {} from connection {}", entityId, connectionId)
                operation
            }

            // ── OperationSet ──────────────────────────────────────────────────────
            // Expects: { command: "operationSet", failurePolicy: "ABORT"|"ROLLBACK"|"CONTINUE",
            //            steps: [ { action: "MUTATE"|"FUNCTION_CALL"|"ASSERT"|"TRIGGER",
            //                        entityId: "...", delta: {...}, function: "..." }, ... ] }
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
                                .entityType(SetTestEntity::class)
                                .action(OperationType.MUTATE)
                                .delta(step.getJsonObject("delta") ?: JsonObject())
                        )
                        "CREATE" -> set.add(
                            Operation()
                                .entityType(SetTestEntity::class)
                                .action(OperationType.CREATE)
                                .delta(step.getJsonObject("delta") ?: JsonObject())
                        )
                        "DELETE" -> set.add(
                            Operation()
                                .entity(entityId ?: continue)
                                .entityType(SetTestEntity::class)
                                .action(OperationType.DELETE)
                                .delta(JsonObject())
                        )
                        "FUNCTION_CALL" -> set.add(
                            FunctionCall()
                                .entity(entityId ?: continue)
                                .entityType(SetTestEntity::class)
                                .function(step.getString("function") ?: "")
                                .also { fc -> step.getJsonObject("args")?.let { fc.args(it) } }
                        )
                        "ASSERT" -> set.add(
                            Assert()
                                .entity(entityId ?: continue)
                                .entityType(SetTestEntity::class)
                                .function(step.getString("function") ?: "")
                                .also { a -> step.getJsonObject("args")?.let { a.args(it) } }
                        )
                        "TRIGGER" -> set.add(
                            Trigger()
                                .entity(entityId ?: continue)
                                .entityType(SetTestEntity::class)
                                .function(step.getString("function") ?: "")
                        )
                        else -> log.warn("Unknown step action in operationSet command: {}", step.getString("action"))
                    }
                }

                if (set.isEmpty()) {
                    log.warn("operationSet command produced an empty set — skipping")
                    return null
                }

                set
            }

            else -> {
                log.warn("Unknown command received: {} from connection: {}", command, connectionId)
                null
            }
        }
    }

    /**
     *
     */
    override suspend fun afterCreateOperation(operation: JsonObject, entity: Entity) {
        val defaultChannel = channelController.getDefaultChannel() ?: throw Exception("No default channel configured")

        channelController.addEntity(entity, defaultChannel)
    }

    override suspend fun afterDeleteOperation(operation: JsonObject, entity: Entity) {
        val defaultChannel = channelController.getDefaultChannel() ?: throw Exception("No default channel configured")

        channelController.removeEntity(entity, defaultChannel)
    }
}