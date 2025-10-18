package com.minare.core.utils.debug

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.operation.models.Operation
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.core.json.JsonObject

@Singleton
class DebugLogger @Inject constructor(
    private val operationDebugUtils: OperationDebugUtils
) {
    private val log = LoggerFactory.getLogger(DebugLogger::class.java)

    /**  Mind over matter won't stop all your chatter */
    private val isEnabled: Map<Type, Boolean> = mapOf(
        Type.NONE to false,
        Type.COORDINATOR_STATE_WORKER_FRAME_COMPLETE to false,
        Type.COORDINATOR_STATE_RESET_SESSION to false,
        Type.COORDINATOR_SESSION_ANNOUNCEMENT to true,
        Type.COORDINATOR_MANIFEST_TIMER_BLOCKED_TICK to false,
        Type.COORDINATOR_WORKER_FRAME_COMPLETE_EVENT to false,
        Type.COORDINATOR_ON_FRAME_COMPLETE_CALLED to false,
        Type.COORDINATOR_ON_FRAME_COMPLETE_BLOCKED to false,
        Type.COORDINATOR_NEXT_FRAME_EVENT to false,
        Type.COORDINATOR_PREPARE_PENDING_MANIFESTS to false,
        Type.COORDINATOR_MANIFEST_BUILDER_WROTE_WORKER to false,
        Type.COORDINATOR_MANIFEST_BUILDER_WROTE_ALL to false,
        Type.COORDINATOR_MANIFEST_BUILDER_ASSIGNED_OPERATIONS to false,
        Type.COORDINATOR_MANIFEST_BUILDER_CLEAR_FRAMES to false,
        Type.COORDINATOR_OPERATION_HANDLER_HANDLE to false,
        Type.COORDINATOR_OPERATION_HANDLER_EXTRACT_BUFFERED to false,
        Type.COORDINATOR_OPERATION_HANDLER_EXTRACTED_OPS to false,
        Type.COORDINATOR_OPERATION_HANDLER_ASSIGN_OPERATION to false,
        Type.COORDINATOR_OPERATION_HANDLER_ASSIGN_LATE_OPERATION to false
    )

    fun log(type: Type, args: List<Any>) {
        if (isEnabled[type] == false) return

        val message: String = when (type) {
            Type.NONE -> { return }
            Type.COORDINATOR_STATE_WORKER_FRAME_COMPLETE -> { "Worker ${args[0]} completed logical frame ${args[1]}" }
            Type.COORDINATOR_STATE_RESET_SESSION -> { "Started new session at timestamp ${args[0]} (nanos: ${args[1]})" }
            Type.COORDINATOR_SESSION_ANNOUNCEMENT -> { "Frame coordinator announced new session ${args[0]}" }
            Type.COORDINATOR_MANIFEST_TIMER_BLOCKED_TICK -> { "Blocked manifest prep timer due to pause state ${args[0]}" }
            Type.COORDINATOR_WORKER_FRAME_COMPLETE_EVENT -> {
                val vlog = args[0] as VerticleLogger
                    vlog.logInfo("Frame ${args[3]} progress: ${args[1]}/${args[2]} workers complete")
                vlog.getEventLogger().trace(
                    "ALL_WORKERS_COMPLETE",
                    mapOf(
                        "logicalFrame" to args[3],
                        "workerCount" to args[2]
                    ),
                    args[4].toString()
                )
                return
            }
            Type.COORDINATOR_ON_FRAME_COMPLETE_CALLED -> { "Logical frame ${args[0]} completed successfully" }
            Type.COORDINATOR_ON_FRAME_COMPLETE_BLOCKED -> { "Completed frame ${args[0]}, stopping due to pause ${args[1]}" }
            Type.COORDINATOR_NEXT_FRAME_EVENT -> { "Broadcasting next frame event after completing frame ${args[0]}" }
            Type.COORDINATOR_PREPARE_PENDING_MANIFESTS -> { "Delayed preparing frames from ${args[0]} due to pause ${args[1]}" }
            Type.COORDINATOR_MANIFEST_BUILDER_WROTE_WORKER -> { "Wrote manifest for worker ${args[0]} with ${args[1]} operations for logical frame ${args[2]}" }
            Type.COORDINATOR_MANIFEST_BUILDER_WROTE_ALL -> { "Created manifests for logical frame ${args[0]} with ${args[1]} total operations distributed to ${args[2]} workers" }
            Type.COORDINATOR_MANIFEST_BUILDER_ASSIGNED_OPERATIONS -> { "Assigned operation ${args[0]} to existing manifest for ${args[1]}" }
            Type.COORDINATOR_MANIFEST_BUILDER_CLEAR_FRAMES -> { "Cleared ${args[0]} manifests for frame ${args[1]}" }
            Type.COORDINATOR_MANIFEST_BUILDER_CLEAR_ALL -> { "Cleared ${args[0]} manifests from distributed map for new session" }
            Type.COORDINATOR_OPERATION_HANDLER_HANDLE -> {
                "OperationHandler.handle(operation): ${args[0]} frameInProgress = ${args[1]} - timestamp = ${args[2]}"
            }
            Type.COORDINATOR_OPERATION_HANDLER_EXTRACT_BUFFERED -> { "OperationHandler.extractBuffered(): oldFrame = ${args[0]}" }
            Type.COORDINATOR_OPERATION_HANDLER_EXTRACTED_OPS -> { "Extracted operations from old frames: ${args[0]}" }
            Type.COORDINATOR_OPERATION_HANDLER_ASSIGN_OPERATION -> {
                "OperationHandler.assignBuffered() handle ${args[0]} - calculatedFrame = ${args[1]} - timestamp = ${args[2]}"
            }
            Type.COORDINATOR_OPERATION_HANDLER_ASSIGN_LATE_OPERATION -> {
                "OperationHandler.assignBuffered() bufferOperation ${args[0]} - calculatedFrame = ${args[1]} - timestamp = ${args[2]}"
            }
        }

        log.info(message)
    }

    companion object {
        enum class Type {
            NONE,
            COORDINATOR_STATE_WORKER_FRAME_COMPLETE,
            COORDINATOR_STATE_RESET_SESSION,
            COORDINATOR_SESSION_ANNOUNCEMENT,
            COORDINATOR_MANIFEST_TIMER_BLOCKED_TICK,
            COORDINATOR_WORKER_FRAME_COMPLETE_EVENT,
            COORDINATOR_ON_FRAME_COMPLETE_CALLED,
            COORDINATOR_ON_FRAME_COMPLETE_BLOCKED,
            COORDINATOR_NEXT_FRAME_EVENT,
            COORDINATOR_PREPARE_PENDING_MANIFESTS,
            COORDINATOR_MANIFEST_BUILDER_WROTE_WORKER,
            COORDINATOR_MANIFEST_BUILDER_WROTE_ALL,
            COORDINATOR_MANIFEST_BUILDER_ASSIGNED_OPERATIONS,
            COORDINATOR_MANIFEST_BUILDER_CLEAR_FRAMES,
            COORDINATOR_MANIFEST_BUILDER_CLEAR_ALL,
            COORDINATOR_OPERATION_HANDLER_HANDLE,
            COORDINATOR_OPERATION_HANDLER_EXTRACT_BUFFERED,
            COORDINATOR_OPERATION_HANDLER_EXTRACTED_OPS,
            COORDINATOR_OPERATION_HANDLER_ASSIGN_OPERATION,
            COORDINATOR_OPERATION_HANDLER_ASSIGN_LATE_OPERATION
        }
    }
}