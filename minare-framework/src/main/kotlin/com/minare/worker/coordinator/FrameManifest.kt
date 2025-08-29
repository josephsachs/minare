package com.minare.worker.coordinator

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

    /**
     * Convert to JsonObject for storage/transmission
     */
    fun toJson(): JsonObject {
        return JsonObject()
            .put("workerId", workerId)
            .put("logicalFrame", logicalFrame)
            .put("createdAt", createdAt)
            .put("operations", JsonArray(operations))
    }

    /**
     * Get the count of operations in this manifest
     */
    fun operationCount(): Int = operations.size

    /**
     * Get all operation IDs in this manifest
     */
    fun getOperationIds(): List<String> {
        return operations.mapNotNull { it.getString("id") }
    }

    /**
     * Check if this manifest contains a specific operation
     */
    fun containsOperation(operationId: String): Boolean {
        return operations.any { it.getString("id") == operationId }
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

        /**
         * Parse frame start time from a manifest key
         */
        fun parseFrameFromKey(key: String): Long? {
            val parts = key.split(":")
            return if (parts.size == 3 && parts[0] == "manifest") {
                parts[1].toLongOrNull()
            } else {
                null
            }
        }

        /**
         * Parse worker ID from a manifest key
         */
        fun parseWorkerFromKey(key: String): String? {
            val parts = key.split(":")
            return if (parts.size == 3 && parts[0] == "manifest") {
                parts[2]
            } else {
                null
            }
        }
    }
}