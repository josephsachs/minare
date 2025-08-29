package com.minare.controller

import com.minare.operation.MessageQueue
import com.minare.operation.Operation
import com.minare.operation.OperationSet
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import com.minare.exception.BackpressureException

/**
 * Controller for the operation event queue.
 * Handles preparing Operations and OperationSets to be queued,
 * and queueing them.
 *
 * This follows the framework pattern:
 * - Framework provides this open base class
 * - Applications must bind it in their module (to this class or their extension)
 * - Applications can extend this class to customize behavior
 */
abstract class OperationController @Inject constructor(
    private val messageQueue: MessageQueue
) {
    private val log = LoggerFactory.getLogger(OperationController::class.java)

    /**
     * Process an incoming message from the WebSocket.
     * This is the entry point for messages from MessageHandler.
     *
     * @param message The raw message from the client
     */
    suspend fun process(message: JsonObject) {
        // Pass message to application layer for packaging into Operations
        val operations = preQueue(message)

        // If application returns nothing, proceed
        if (operations == null) {
            log.debug("Application preQueue returned null, skipping message")
            return
        }

        queue(operations)
    }

    /**
     * Queue operations to Kafka.
     * Converts Operations/OperationSets to JsonArray format and sends them.
     *
     * @param operations Either an Operation or OperationSet
     */
    private suspend fun queue(operations: Any) {
        val message = when (operations) {
            is OperationSet -> operations.toJsonArray()
            is Operation -> JsonArray().add(operations.build())
            else -> throw IllegalArgumentException("Expected Operation or OperationSet, got ${operations::class.simpleName}")
        }

        sendMessage(message)
        postQueue(message)
    }

    /**
     * Application developer override hook.
     * Convert incoming client messages to Operations.
     *
     * @param message The raw message from the client
     * @return Operation, OperationSet, or null to skip processing
     */
    protected abstract suspend fun preQueue(message: JsonObject): Any?

    /**
     * Application developer override hook.
     * Called after messages have been sent to Kafka.
     *
     * @param operations The JsonArray that was sent to Kafka
     */
    protected open suspend fun postQueue(operations: JsonArray) {
        // Default implementation does nothing
    }

    /**
     * Send an operation set to the message broker
     *
     * @param message JsonArray of operations
     */
    private suspend fun sendMessage(message: JsonArray) {
        if (message.isEmpty) {
            log.debug("Skipping empty operation set")
            return
        }

        // TODO: Re-enable BackpressureManager with proper triggering conditions
        //if (backpressureManager.isActive()) {
        //    throw BackpressureException("System overloaded - please retry")
        //}

        log.debug("Sending {} operations to Kafka topic {}", message.size(), OPERATIONS_TOPIC)
        messageQueue.send(OPERATIONS_TOPIC, message)
    }

    companion object {
        private const val OPERATIONS_TOPIC = "minare.operations"
    }
}