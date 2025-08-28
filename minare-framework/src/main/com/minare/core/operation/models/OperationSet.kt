package com.minare.core.operation.models

import io.vertx.core.json.JsonArray

class OperationSet {
    private val operations = JsonArray()

    fun add(operation: Operation) = apply {
        operations.add(operation.build())
    }

    internal fun toJsonArray(): JsonArray = operations

    fun isEmpty(): Boolean = operations.isEmpty

    fun size(): Int = operations.size()
}