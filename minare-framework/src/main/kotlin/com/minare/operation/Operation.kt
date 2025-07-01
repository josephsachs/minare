package com.minare.operation

import io.vertx.core.json.JsonObject

class Operation {
    private var entityId: String? = null
    private var actionType: OperationType? = null
    private val values = JsonObject()
    private var version: Long? = null

    fun entity(id: String) = apply {
        entityId = id
    }

    fun action(type: OperationType) = apply {
        actionType = type
    }

    fun value(key: String, value: Any) = apply {
        values.put(key, value)
    }

    fun delta(changes: JsonObject) = apply {
        values.mergeIn(changes)
    }

    fun version(v: Long) = apply {
        version = v
    }

    internal fun build(): JsonObject {
        requireNotNull(entityId) { "Entity ID is required" }
        requireNotNull(actionType) { "Action type is required" }

        return JsonObject().apply {
            put("entity", entityId)
            put("action", actionType.toString())
            if (!values.isEmpty) {
                put("values", values)
            }
            version?.let { put("version", it) }
        }
    }
}