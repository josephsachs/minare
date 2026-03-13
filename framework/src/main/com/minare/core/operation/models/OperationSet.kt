package com.minare.core.operation.models

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.util.UUID

/**
 * An OperationSet groups related operations that should be routed, ordered,
 * and processed as a unit within a single assigned frame.
 *
 * All member operations are stamped on add() with:
 * - operationSetId: shared UUID identifying the set (routing key for affinity)
 * - setIndex: declaration order within the set (preserved in manifest ordering)
 * - timestamp: normalized to the set's creation time (ensures all members map to the same frame)
 *
 * failurePolicy is stamped on all members at build() time.
 */
class OperationSet {
    val id: String = UUID.randomUUID().toString()
    private val members = JsonArray()
    private var nextIndex = 0
    private val timestamp: Long = System.currentTimeMillis()
    private var sealed = false

    var failurePolicy: FailurePolicy = FailurePolicy.ABORT

    fun failurePolicy(policy: FailurePolicy) = apply { failurePolicy = policy }

    fun add(operation: Operation) = apply {
        check(!sealed) { "OperationSet has already been built" }
        stamp(operation.build())
    }

    fun add(call: FunctionCall) = apply {
        check(!sealed) { "OperationSet has already been built" }
        stamp(call.build())
    }

    fun add(assert: Assert) = apply {
        check(!sealed) { "OperationSet has already been built" }
        stamp(assert.build())
    }

    fun add(trigger: Trigger) = apply {
        check(!sealed) { "OperationSet has already been built" }
        stamp(trigger.build())
    }

    private fun stamp(member: JsonObject) {
        member.put("operationSetId", id)
        member.put("setIndex", nextIndex++)
        member.put("timestamp", timestamp)
        members.add(member)
    }

    /**
     * Seal the set: stamp failurePolicy on all members and return the serialized array.
     * The coordinator infers a JsonArray with >1 items is an OperationSet.
     */
    fun build(): JsonArray {
        sealed = true
        for (i in 0 until members.size()) {
            members.getJsonObject(i).put("failurePolicy", failurePolicy.name)
        }
        return members
    }

    fun isEmpty(): Boolean = members.isEmpty

    fun size(): Int = members.size()
}
