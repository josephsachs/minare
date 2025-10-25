package com.minare.core.utils.debug

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.impl.logging.LoggerFactory

@Singleton
class DebugLogger @Inject constructor() {
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
        Type.COORDINATOR_NEXT_FRAME_EVENT to true,
        Type.COORDINATOR_PREPARE_PENDING_MANIFESTS to false,
        Type.COORDINATOR_MANIFEST_BUILDER_WROTE_WORKER to false,
        Type.COORDINATOR_MANIFEST_BUILDER_WROTE_ALL to false,
        Type.COORDINATOR_MANIFEST_BUILDER_ASSIGNED_OPERATIONS to true,
        Type.COORDINATOR_MANIFEST_BUILDER_CLEAR_FRAMES to false,
        Type.COORDINATOR_OPERATION_HANDLER_HANDLE to true,
        Type.COORDINATOR_OPERATION_HANDLER_EXTRACT_BUFFERED to false,
        Type.COORDINATOR_OPERATION_HANDLER_EXTRACTED_OPS to false,
        Type.COORDINATOR_OPERATION_HANDLER_ASSIGN_OPERATION to true,
        Type.COORDINATOR_OPERATION_HANDLER_ASSIGN_LATE_OPERATION to false,
        Type.COORDINATOR_WORK_DISPATCH_DISTRIBUTE_NO_ITEMS to false,
        Type.COORDINATOR_WORK_DISPATCH_DISTRIBUTE_NO_WORKERS to true,
        Type.CHANNEL_CONTROLLER_ADD_CLIENT_CHANNEL to false,
        Type.CHANNEL_CONTROLLER_ADD_ENTITY_CHANNEL to false,
        Type.CHANNEL_CONTROLLER_ADD_ENTITIES_CHANNEL to false,
        Type.CHANNEL_CONTROLLER_CREATE_CHANNEL to true,
        Type.CONNECTION_CONTROLLER_CREATE_CONNECTION to false,
        Type.CONNECTION_CONTROLLER_FOUND_CONNECTION to false,
        Type.CONNECTION_CONTROLLER_STORED_CONNECTION to false,
        Type.CONNECTION_CONTROLLER_UPDATE_CONNECTION to false,
        Type.CONNECTION_CONTROLLER_UPDATE_SOCKETS to false,
        Type.CONNECTION_CONTROLLER_UPSOCKET_DISCONNECT to false,
        Type.CONNECTION_CONTROLLER_CONNECTION_DELETED to false,
        Type.CONNECTION_CONTROLLER_REMOVE_UPSOCKET to false,
        Type.CONNECTION_CONTROLLER_REMOVE_DOWNSOCKET to false,
        Type.CONNECTION_CONTROLLER_UPSOCKET_CLOSED to false,
        Type.CONNECTION_CONTROLLER_CLEANUP_CONNECTION to false,
        Type.CONNECTION_CONTROLLER_ALREADY_DELETED_WARNING to false,
        Type.OPERATION_CONTROLLER_PROCESS_MESSAGE to true,
        Type.OPERATION_CONTROLLER_QUEUE to true,
        Type.OPERATION_CONTROLLER_SEND_MESSAGE to true
    )

    fun log(type: Type, args: List<Any?> = listOf()) {
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
            Type.COORDINATOR_WORK_DISPATCH_DISTRIBUTE_NO_ITEMS -> { "WorkDispatcher with strategy RANGE received no items, returning empty map" }
            Type.COORDINATOR_WORK_DISPATCH_DISTRIBUTE_NO_WORKERS -> { "WorkUnit did not distribute because no workers were available, returning empty map" }
            Type.CHANNEL_CONTROLLER_ADD_CLIENT_CHANNEL -> { "Client ${args[0]} subscribed to channel ${args[1]}" }
            Type.CHANNEL_CONTROLLER_ADD_ENTITY_CHANNEL -> { "Added entity ${args[0]} to channel ${args[1]} with context ${args[2]}" }
            Type.CHANNEL_CONTROLLER_ADD_ENTITIES_CHANNEL -> { "Added ${args[0]} out of ${args[1]} entities to channel ${args[2]}" }
            Type.CHANNEL_CONTROLLER_CREATE_CHANNEL -> { "ChannelController creating new channel with ID: ${args[0]}" }
            Type.ENTITY_CONTROLLER_SAVE_ENTITY -> { "Saving existing entity to Redis with key ${args[0]}" }
            Type.CONNECTION_CONTROLLER_CREATE_CONNECTION -> { "Connection created and stored with id ${args[0]} — upSocketId ${args[1]} — downSocketId ${args[2]}" }
            Type.CONNECTION_CONTROLLER_STORED_CONNECTION -> { "Stored un-cached connection in database: id ${args[0]} — upSocketId ${args[1]} — downSocketId ${args[2]}"}
            Type.CONNECTION_CONTROLLER_FOUND_CONNECTION -> { "Connection found in cache: id ${args[0]} — upSocketId ${args[1]} — downSocketId ${args[2]}" }
            Type.CONNECTION_CONTROLLER_UPDATE_CONNECTION -> { "Connection loaded from database to cache: id ${args[0]} — upSocketId ${args[1]} — downSocketId ${args[2]}" }
            Type.CONNECTION_CONTROLLER_UPDATE_SOCKETS -> { "Connection updated transport sockets id ${args[0]} — upSocketId ${args[1]} — downSocketId ${args[2]}"}
            Type.CONNECTION_CONTROLLER_REMOVE_UPSOCKET -> { "Connection ${args[0]} deleted from database" }
            Type.CONNECTION_CONTROLLER_UPSOCKET_DISCONNECT -> { "Up socket for connection ${args[0]} marked as disconnected, available for reconnection" }
            Type.CONNECTION_CONTROLLER_CONNECTION_DELETED -> { "Connection ${args[0]} deleted from database" }
            Type.CONNECTION_CONTROLLER_REMOVE_DOWNSOCKET -> { "Down socket removed for connection ${args[0]}" }
            Type.CONNECTION_CONTROLLER_UPSOCKET_CLOSED -> { "Up socket closed for connection ${args[0]}, marking for potential reconnection" }
            Type.CONNECTION_CONTROLLER_CLEANUP_CONNECTION -> { "Cleaned up connection ${args[0]} from ${args[1]} channels" }
            Type.CONNECTION_CONTROLLER_ALREADY_DELETED_WARNING -> { "Could not delete connection ${args[0]} from database - it may already be deleted\nException message: {e}" }
            Type.OPERATION_CONTROLLER_PROCESS_MESSAGE -> { "Operation controller processing message ${args[0] }" }
            Type.OPERATION_CONTROLLER_QUEUE -> { "Operation controller queueing ${args[0]} containing ${args[1]}" }
            Type.OPERATION_CONTROLLER_SEND_MESSAGE -> { "Operation controller sending message ${args[0]} containing ${args[1]}" }
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
            COORDINATOR_OPERATION_HANDLER_ASSIGN_LATE_OPERATION,
            COORDINATOR_WORK_DISPATCH_DISTRIBUTE_NO_ITEMS,
            COORDINATOR_WORK_DISPATCH_DISTRIBUTE_NO_WORKERS,
            CHANNEL_CONTROLLER_ADD_CLIENT_CHANNEL,
            CHANNEL_CONTROLLER_ADD_ENTITY_CHANNEL,
            CHANNEL_CONTROLLER_ADD_ENTITIES_CHANNEL,
            CHANNEL_CONTROLLER_CREATE_CHANNEL,
            ENTITY_CONTROLLER_SAVE_ENTITY,
            CONNECTION_CONTROLLER_CREATE_CONNECTION,
            CONNECTION_CONTROLLER_FOUND_CONNECTION,
            CONNECTION_CONTROLLER_STORED_CONNECTION,
            CONNECTION_CONTROLLER_UPDATE_CONNECTION,
            CONNECTION_CONTROLLER_UPDATE_SOCKETS,
            CONNECTION_CONTROLLER_UPSOCKET_DISCONNECT,
            CONNECTION_CONTROLLER_CONNECTION_DELETED,
            CONNECTION_CONTROLLER_REMOVE_UPSOCKET,
            CONNECTION_CONTROLLER_REMOVE_DOWNSOCKET,
            CONNECTION_CONTROLLER_UPSOCKET_CLOSED,
            CONNECTION_CONTROLLER_CLEANUP_CONNECTION,
            CONNECTION_CONTROLLER_ALREADY_DELETED_WARNING,
            OPERATION_CONTROLLER_PROCESS_MESSAGE,
            OPERATION_CONTROLLER_QUEUE,
            OPERATION_CONTROLLER_SEND_MESSAGE,
        }
    }
}