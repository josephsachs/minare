package com.minare.integration.controller

import com.google.inject.Inject
import com.minare.controller.OperationController
import com.minare.core.entity.models.Entity
import com.minare.core.operation.models.Operation
import com.minare.core.operation.models.OperationType
import com.minare.integration.models.Node
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.UUID
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

            "operation" -> {
                // Passthrough for pre-built operations (e.g. from OperationSet tests).
                // Reconstructs an Operation, preserving set metadata (operationSetId,
                // setIndex, failurePolicy) via the values merge path in Operation.build().
                val opJson = message.getJsonObject("operation")
                if (opJson == null) {
                    log.warn("Invalid operation command: missing operation payload")
                    return null
                }
                val actionStr = opJson.getString("action")
                val actionType = try { OperationType.valueOf(actionStr) } catch (_: Exception) { null }
                if (actionType == null) {
                    log.warn("Unsupported action type in operation passthrough: {}", actionStr)
                    return null
                }

                val operation = Operation()
                operation.id = opJson.getString("id") ?: UUID.randomUUID().toString()
                operation.entity(opJson.getString("entityId"))
                operation.entityType(actionStr.let { opJson.getString("entityType") ?: "Node" })
                operation.action(actionType)
                operation.delta(opJson.getJsonObject("delta") ?: JsonObject())
                operation.timestamp(opJson.getLong("timestamp", System.currentTimeMillis()))
                opJson.getLong("version")?.let { operation.version(it) }
                opJson.getString("meta")?.let { operation.meta(it) }

                // Carry through non-standard fields so operationSetId, setIndex,
                // failurePolicy survive Operation.build() → mergeIn(values)
                val standardFields = setOf("id", "entityId", "entityType", "action",
                    "delta", "version", "timestamp", "meta")
                opJson.fieldNames().filter { it !in standardFields }.forEach { field ->
                    operation.value(field, opJson.getValue(field))
                }

                log.debug("Passthrough operation {} action={} setId={}",
                    operation.id, actionStr, opJson.getString("operationSetId"))
                operation
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