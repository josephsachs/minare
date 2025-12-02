package com.minare.worker.coordinator.models

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.io.Serializable

/**
 * Frame manifest data structure for distributed map storage.
 * Contains all operations assigned to a specific worker for a frame.
 */
data class FrameManifest(
    val workerId: String,
    val logicalFrame: Long,
    val createdAt: Long,
    val operations: List<JsonObject>
) : Serializable {
    fun toJson(): JsonObject {
        return JsonObject()
            .put("workerId", workerId)
            .put("logicalFrame", logicalFrame)
            .put("createdAt", createdAt)
            .put("operations", JsonArray(operations))
    }

    companion object {
        /**
         * Create a FrameManifest from a JsonObject
         */
        fun fromJson(json: JsonObject): FrameManifest {
            val operations = json.getJsonArray("operations", JsonArray())
                .filterIsInstance<JsonObject>()

            return FrameManifest(
                workerId = json.getString("workerId")
                    ?: throw IllegalArgumentException("Missing workerId"),
                logicalFrame = json.getLong("logicalFrame")
                    ?: throw IllegalArgumentException("Missing logicalFrame"),
                createdAt = json.getLong("createdAt")
                    ?: throw IllegalArgumentException("Missing frameEndTime"),
                operations = operations
            )
        }

        /**
         * Create the distributed map key for a manifest
         */
        fun makeKey(logicalFrame: Long, workerId: String): String {
            return "manifest:$logicalFrame:$workerId"
        }
    }
}