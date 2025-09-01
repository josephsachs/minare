package com.minare.core.frames.models

import io.vertx.core.json.JsonObject
import com.minare.core.operation.models.OperationType

data class FrameDelta(
    val frameNumber: Long,
    val entityId: String,
    val operation: OperationType,
    val before: JsonObject?,
    val after: JsonObject?,
    val version: Long,
    val timestamp: Long,
    val operationId: String
) {
    fun toJson(): JsonObject {
        return JsonObject()
            .put("frameNumber", frameNumber)
            .put("entityId", entityId)
            .put("operation", operation.toString())
            .put("before", before)
            .put("after", after)
            .put("version", version)
            .put("timestamp", timestamp)
            .put("operationId", operationId)
    }

    companion object {
        fun fromJson(json: JsonObject): FrameDelta {
            return FrameDelta(
                frameNumber = json.getLong("frameNumber"),
                entityId = json.getString("entityId"),
                operation = OperationType.valueOf(json.getString("operation")),
                before = json.getJsonObject("before"),
                after = json.getJsonObject("after"),
                version = json.getLong("version"),
                timestamp = json.getLong("timestamp"),
                operationId = json.getString("operationId")
            )
        }
    }
}