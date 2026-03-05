package com.minare.nodegraph.controller

import com.fasterxml.jackson.databind.ObjectMapper
import com.minare.controller.OperationController
import com.minare.core.operation.models.Operation
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
class NodeGraphOperationController @Inject constructor() : OperationController() {
    private val log = LoggerFactory.getLogger(NodeGraphOperationController::class.java)

    /**
     * Convert incoming client commands to Operations before queueing to Kafka.
     *
     * @param message The raw message from the client
     * @return Operation, OperationSet, or null to skip processing
     */
    override suspend fun preQueue(message: JsonObject): Any? {
        log.info("DEBUG_PREQUEUE: Entry with $message")

        val command = message.getString("command")

        log.info("DEBUG_PREQUEUE: command $command")

        return when (command) {
            "mutate" -> {
                // Convert mutate command to Operation
                val entityObject = message.getJsonObject("entity")

                log.info("DEBUG_PREQUEUE: entityObject $entityObject")

                if (entityObject == null) {
                    log.warn("Invalid mutate command: missing entity object")
                    return null
                }

                val entityId = entityObject.getString("_id")

                log.info("DEBUG_PREQUEUE: entityObject $entityId")

                if (entityId == null) {
                    log.warn("Invalid mutate command: missing entity ID")
                    return null
                }

                val operation = Operation()
                    .entity(entityId)
                    .action(OperationType.MUTATE)

                log.info("DEBUG_PREQUEUE: initialized operation ${operation}")

                // Build the delta, enriched with lastOperation record.
                // The operation ID and timestamp are already determined;
                // frame number is not yet assigned (happens in the coordinator)
                // so we omit it — the client can correlate via the metrics channel.
                val state = entityObject.getJsonObject("state") ?: JsonObject()

                log.info("DEBUG_PREQUEUE: created state ${state}")

                state.put("lastOperation", operation)

                log.info("DEBUG_PREQUEUE: appended to ${state.getString("lastOperation")}")

                operation.entityType(Node::class)
                entityObject.getLong("version")?.let { operation.version(it) }
                operation.delta(state)

                log.info("DEBUG_PREQUEUE: set Operation.`delta` with $state")

                /**
                 * NodeGraph only deals with Nodes, application must perform dispatch if more
                 * than one type. For more complicated entity schemes decomposition of preQueue
                 * into specific handler services is recommended.
                 */

                log.debug("Created MUTATE operation ${operation.getId()}")

                log.info("DEBUG_PREQUEUE: created final $operation ******* FINAL")

                operation
            }

            // Add other command types here as needed
            // "create" -> { ... }
            // "delete" -> { ... }

            else -> {
                null
            }
        }
    }
}