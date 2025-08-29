package com.minare.worker.coordinator

import io.vertx.core.json.JsonObject
import java.io.Serializable

/**
 * Tracks the completion of a single operation by a worker.
 * Stored in distributed map for fault tolerance during frame processing.
 */
data class OperationCompletion(
    val operationId: String,
    val workerId: String,
    val completedAt: Long = System.currentTimeMillis(),
    val entityId: String? = null,
    val resultSummary: String? = null
) : Serializable {

    /**
     * Convert to JsonObject for storage/transmission
     */
    fun toJson(): JsonObject {
        val json = JsonObject()
            .put("operationId", operationId)
            .put("workerId", workerId)
            .put("completedAt", completedAt)

        entityId?.let { json.put("entityId", it) }
        resultSummary?.let { json.put("resultSummary", it) }

        return json
    }

    /**
     * Get the duration since completion
     */
    fun ageMs(): Long = System.currentTimeMillis() - completedAt

    companion object {
        /**
         * Create an OperationCompletion from a JsonObject
         */
        fun fromJson(json: JsonObject): OperationCompletion {
            return OperationCompletion(
                operationId = json.getString("operationId")
                    ?: throw IllegalArgumentException("Missing operationId"),
                workerId = json.getString("workerId")
                    ?: throw IllegalArgumentException("Missing workerId"),
                completedAt = json.getLong("completedAt", System.currentTimeMillis()),
                entityId = json.getString("entityId"),
                resultSummary = json.getString("resultSummary")
            )
        }

        /**
         * Create the distributed map key for a completion
         */
        fun makeKey(frameStartTime: Long, operationId: String): String {
            return "frame-$frameStartTime:op-$operationId"
        }

        /**
         * Parse frame start time from a completion key
         */
        fun parseFrameFromKey(key: String): Long? {
            val match = Regex("frame-(\\d+):op-.*").matchEntire(key)
            return match?.groupValues?.get(1)?.toLongOrNull()
        }

        /**
         * Parse operation ID from a completion key
         */
        fun parseOperationFromKey(key: String): String? {
            val match = Regex("frame-\\d+:op-(.*)").matchEntire(key)
            return match?.groupValues?.get(1)
        }

        /**
         * Create a batch completion summary for monitoring
         */
        fun summarize(completions: Collection<OperationCompletion>): JsonObject {
            val byWorker = completions.groupBy { it.workerId }

            return JsonObject().apply {
                put("totalOperations", completions.size)
                put("workerCount", byWorker.size)
                put("workerSummary", JsonObject().apply {
                    byWorker.forEach { (workerId, ops) ->
                        put(workerId, ops.size)
                    }
                })

                // Calculate timing stats
                if (completions.isNotEmpty()) {
                    val times = completions.map { it.completedAt }
                    put("earliestCompletion", times.minOrNull())
                    put("latestCompletion", times.maxOrNull())
                    put("durationMs", (times.maxOrNull() ?: 0) - (times.minOrNull() ?: 0))
                }
            }
        }
    }
}