package com.minare.worker.coordinator

import com.minare.time.FrameConfiguration
import com.minare.utils.VerticleLogger
import io.vertx.kotlin.coroutines.CoroutineVerticle
import javax.inject.Inject

/**
 * Verticle responsible for coordinating frames, checking instance availability
 * and managing the backup and recovery process
 */
class CoordinatorVerticle @Inject constructor(
    private val vlog: VerticleLogger,
    private val frameConfig: FrameConfiguration
) : CoroutineVerticle() {
    companion object {
        const val ADDRESS_FRAME_START = "minare.coordinator.frame.start"
        const val ADDRESS_FRAME_PAUSE = "minare.coordinator.frame.pause"
        const val ADDRESS_RESYNC_CLOCKS = "minare.coordinator.frame.resync"

        const val ADDRESS_COORDINATOR_OPERATION_MANIFEST = "minare.coordinator.operation.manifest"
        const val ADDRESS_WORKER_FRAME_STARTED = "minare.worker.frame.started"
        const val ADDRESS_WORKER_FRAME_COMPLETE = "minare.worker.frame.complete"
        const val ADDRESS_WORKER_REGISTER = "minare.coordinator.worker.register"
        const val ADDRESS_WORKER_HEARTBEAT = "minare.coordinator.worker.heartbeat"

        const val ADDRESS_COORDINATOR_BACKUP = "minare.coordinator.state.backup"
        const val ADDRESS_COORDINATOR_ROLLBACK = "minare.coordinator.state.rollback"
        const val ADDRESS_COORDINATOR_REPLAY = "minare.coordinator.state.replay"
    }
}