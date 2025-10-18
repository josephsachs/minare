package com.minare.core.utils.debug

import com.google.inject.Singleton
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.impl.logging.LoggerFactory

@Singleton
class DebugLogger {
    private val log = LoggerFactory.getLogger(DebugLogger::class.java)

    /**  Mind over matter won't stop all your chatter */
    private val isEnabled: Map<Type, Boolean> = mapOf(
        Type.NONE to false,
        Type.COORDINATOR_STATE_WORKER_FRAME_COMPLETE to true,
        Type.COORDINATOR_STATE_RESET_SESSION to true,
        Type.COORDINATOR_SESSION_ANNOUNCEMENT to true,
        Type.COORDINATOR_MANIFEST_TIMER_BLOCKED_TICK to true,
        Type.COORDINATOR_WORKER_FRAME_COMPLETE_EVENT to true
    )

    fun log(type: Type, args: List<Any>) {
        if (isEnabled[type] == false) return

        when (type) {
            Type.NONE -> {}
            Type.COORDINATOR_STATE_WORKER_FRAME_COMPLETE -> {  log.info("Worker ${args[0]} completed logical frame ${args[1]}") }
            Type.COORDINATOR_STATE_RESET_SESSION -> { log.info("Started new session at timestamp ${args[0]} (nanos: ${args[1]})",) }
            Type.COORDINATOR_SESSION_ANNOUNCEMENT -> { log.info("Frame coordinator announced new session ${args[0]}") }
            Type.COORDINATOR_MANIFEST_TIMER_BLOCKED_TICK -> { log.info("Blocked manifest prep timer due to pause state ${args[0]}") }
            Type.COORDINATOR_WORKER_FRAME_COMPLETE_EVENT -> {
                val vlog = args[0] as VerticleLogger
                    vlog.logInfo("Frame ${args[1]} progress: ${args[2]}/${args[3]} workers complete")
                vlog.getEventLogger().trace(
                    "ALL_WORKERS_COMPLETE",
                    mapOf(
                        "logicalFrame" to args[3],
                        "workerCount" to args[4]
                    ),
                    args[5].toString()
                )
            }
            Type.COORDINATOR_ON_FRAME_COMPLETE_CALLED -> { log.info("Logical frame ${args[0]} completed successfully") }
            Type.COORDINATOR_ON_FRAME_COMPLETE_BLOCKED -> { log.info("Completed frame ${args[0]}, stopping due to pause ${args[1]}")}
            Type.COORDINATOR_NEXT_FRAME_EVENT -> { log.info("Broadcasting next frame event after completing frame ${args[0]}") }
            Type.COORDINATOR_PREPARE_PENDING_MANIFESTS -> { "Delayed preparing frames from ${args[0]} due to pause ${args[1]}"}
        }
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
            COORDINATOR_PREPARE_PENDING_MANIFESTS
        }
    }
}