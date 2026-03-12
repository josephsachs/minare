package com.minare.core.operation.models

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.*

/**
 * An OperationSet groups related operations that should be routed, ordered,
 * and (eventually) processed as a unit.
 *
 * All member operations are stamped with:
 * - operationSetId: shared UUID identifying the set (used as routing key for affinity)
 * - setIndex: declaration order within the set (preserved in manifest ordering)
 * - timestamp: normalized to the set's creation time (ensures all members map to the same frame)
 */
class OperationSet {
    val id: String = UUID.randomUUID().toString()
    private val operations = JsonArray()
    private var nextIndex = 0
    private val timestamp: Long = System.currentTimeMillis()

    fun add(operation: OperationSetMember) = apply {
        val built = operation.build()
        built.put("operationSetId", id)
        built.put("setIndex", nextIndex++)
        built.put("timestamp", timestamp)
        operations.add(built)
    }

    internal fun toJsonArray(): JsonArray = operations

    fun isEmpty(): Boolean = operations.isEmpty

    fun size(): Int = operations.size()

    /**
     * Controls worker behavior when a step in the set fails:
     * - CONTINUE: proceed regardless (default)
     * - ABORT: halt remaining steps, applied deltas stand
     * - ROLLBACK: halt remaining steps, undo applied deltas before yielding
     */
    enum class FailurePolicy { CONTINUE, ABORT, ROLLBACK }

    private var failurePolicy: FailurePolicy = FailurePolicy.CONTINUE

    fun onFailure(policy: FailurePolicy) = apply { this.failurePolicy = policy }

    /**
     * Declared deltas from member mutations, collected at build time.
     * Used as the rollback buffer if failurePolicy is ROLLBACK.
     */
    private var deltas: Array<JsonObject> = arrayOf()
}