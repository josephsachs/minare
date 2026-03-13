package com.minare.core.operation.models

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import lombok.Setter
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

    fun toJsonArray(): JsonArray = operations

    fun isEmpty(): Boolean = operations.isEmpty

    fun size(): Int = operations.size()

    enum class FailurePolicy { CONTINUE, ABORT, ROLLBACK }

    var failurePolicy: FailurePolicy = FailurePolicy.CONTINUE



    /**
     * Declared deltas from member mutations, collected at build time.
     * Used as the rollback buffer if failurePolicy is ROLLBACK.
     */
    private var deltas: Array<JsonObject> = arrayOf()
}