package com.minare.controller

import com.minare.operation.MessageQueue
import com.minare.operation.Operation
import com.minare.operation.OperationSet
import io.vertx.core.json.JsonArray
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

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
@Singleton
abstract class OperationController @Inject constructor(
    private val messageQueue: MessageQueue
) {
    private val log = LoggerFactory.getLogger(EntityController::class.java)

    suspend fun queue(operations: Any) {
        val message = when (operations) {
            is OperationSet -> operations.toJsonArray()
            is Operation -> JsonArray().add(operations.build())
            else -> throw IllegalArgumentException("Expected OperationSet or Operation")
        }
        
        preQueue(message)
            .also { sendMessage(it) }
            .let { postQueue(it) }
    }

    /**
     * Developer override hooks
     */
    protected open suspend fun preQueue(operations: JsonArray): JsonArray { return operations }
    protected open suspend fun postQueue(operations: JsonArray): JsonArray { return operations }

    /**
     * Send an operation set to the message broker
     *
     * @param message
     * @return if the operation succeeded
     */
    internal suspend fun sendMessage(message: JsonArray) {
        messageQueue.send(OPERATIONS_TOPIC, message)
    }

    companion object {
        private const val OPERATIONS_TOPIC = "minare.operations"
    }
}