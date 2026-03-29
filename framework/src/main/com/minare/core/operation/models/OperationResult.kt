package com.minare.core.operation.models

import io.vertx.core.json.JsonObject

sealed class OperationResult {
    abstract val entityId: String?
    abstract val operationType: OperationType
    abstract val operationId: String?
    abstract val frameNumber: Long?
    abstract val durationMs: Long

    data class Success(
        override val entityId: String?,
        override val operationType: OperationType,
        override val operationId: String?,
        override val frameNumber: Long?,
        override val durationMs: Long
    ) : OperationResult()

    data class Rejected(
        override val entityId: String?,
        override val operationType: OperationType,
        override val operationId: String?,
        override val frameNumber: Long?,
        override val durationMs: Long,
        val reason: String
    ) : OperationResult()

    data class Failed(
        override val entityId: String?,
        override val operationType: OperationType,
        override val operationId: String?,
        override val frameNumber: Long?,
        override val durationMs: Long,
        val cause: Exception
    ) : OperationResult()

    fun toJson(): JsonObject {
        val json = JsonObject()
            .put("operationType", operationType.name)
            .put("entityId", entityId)
            .put("operationId", operationId)
            .put("frameNumber", frameNumber)
            .put("durationMs", durationMs)

        when (this) {
            is Success -> json.put("status", "success").put("success", true)
            is Rejected -> json.put("status", "rejected").put("reason", reason)
            is Failed -> json.put("status", "failed")
                .put("error", cause.message)
                .put("errorType", cause.javaClass.simpleName)
        }

        return json
    }

    companion object {
        const val ADDRESS_OPERATION_RESULT = "minare.operation.result"

        fun formatFailure(result: OperationResult): String {
            val sb = StringBuilder()
            sb.appendLine("════════════════════════════════════════════════════════════")
            sb.appendLine(" MINARE FRAMEWORK — OPERATION ${result.operationType}")

            when (result) {
                is Rejected -> {
                    sb.appendLine("────────────────────────────────────────────────────────────")
                    sb.appendLine(" Status:     REJECTED")
                    sb.appendLine(" Entity:     ${result.entityId ?: "(none)"}")
                    sb.appendLine(" Operation:  ${result.operationId ?: "(none)"}")
                    sb.appendLine(" Frame:      ${result.frameNumber ?: "(none)"}")
                    sb.appendLine(" Duration:   ${result.durationMs}ms")
                    sb.appendLine("────────────────────────────────────────────────────────────")
                    sb.appendLine(" ${result.reason}")
                }
                is Failed -> {
                    sb.appendLine("────────────────────────────────────────────────────────────")
                    sb.appendLine(" Status:     INTERNAL ERROR")
                    sb.appendLine(" Entity:     ${result.entityId ?: "(none)"}")
                    sb.appendLine(" Operation:  ${result.operationId ?: "(none)"}")
                    sb.appendLine(" Frame:      ${result.frameNumber ?: "(none)"}")
                    sb.appendLine(" Duration:   ${result.durationMs}ms")
                    sb.appendLine("────────────────────────────────────────────────────────────")
                    sb.appendLine(" ${result.cause.javaClass.name}: ${result.cause.message}")
                    result.cause.stackTrace.take(8).forEach { frame ->
                        sb.appendLine("   at $frame")
                    }
                }
                is Success -> {}
            }

            sb.append("════════════════════════════════════════════════════════════")
            return sb.toString()
        }
    }
}