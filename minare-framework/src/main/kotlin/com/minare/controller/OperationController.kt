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

    /**
     * Developer override hooks
     */
    protected open suspend fun preQueue(commands: JsonArray) {}
    protected open suspend fun postQueue(commands: JsonArray) {}

    /**
     * Queue an operation set for
     *
     * This method handles the MongoDB-first-then-Redis flow for new entities:
     * 1. Save to MongoDB to get an ID assigned
     * 2. Save to Redis for fast state access
     *
     * @param operations
     * @return if the operation succeeded
     */
    // Framework calls this after preQueue
    internal suspend fun queueOperations(operations: Any) {
        val message = when (operations) {
            is OperationSet -> operations.toJsonArray()
            is Operation -> JsonArray().add(operations.build())
            else -> throw IllegalArgumentException("Expected OperationSet or Operation")
        }

        messageQueue.send(OPERATIONS_TOPIC, message)
    }

    companion object {
        private const val OPERATIONS_TOPIC = "minare.operations"
    }
}