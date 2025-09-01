package com.minare.core.frames.coordinator.models

import io.vertx.core.json.JsonObject
import java.io.Serializable

/**
 * Completion report from a worker after processing its snapshot partition.
 * Contains verification data to ensure snapshot integrity.
 */
data class SnapshotCompletion(
    val workerId: String,
    val sessionId: String,
    val entityCount: Int,
    val deltaCount: Int,
    val entitiesChecksum: String?,  // Optional checksum of entity data
    val success: Boolean,
    val errorMessage: String? = null,
    val completedAt: Long = System.currentTimeMillis()
) : Serializable {

    fun toJson(): JsonObject {
        return JsonObject()
            .put("workerId", workerId)
            .put("sessionId", sessionId)
            .put("entityCount", entityCount)
            .put("deltaCount", deltaCount)
            .put("entitiesChecksum", entitiesChecksum)
            .put("success", success)
            .put("errorMessage", errorMessage)
            .put("completedAt", completedAt)
    }

    companion object {
        private const val serialVersionUID = 1L

        fun fromJson(json: JsonObject): SnapshotCompletion {
            return SnapshotCompletion(
                workerId = json.getString("workerId"),
                sessionId = json.getString("sessionId"),
                entityCount = json.getInteger("entityCount", 0),
                deltaCount = json.getInteger("deltaCount", 0),
                entitiesChecksum = json.getString("entitiesChecksum"),
                success = json.getBoolean("success", false),
                errorMessage = json.getString("errorMessage"),
                completedAt = json.getLong("completedAt", System.currentTimeMillis())
            )
        }
    }
}