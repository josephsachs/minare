package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.core.utils.vertx.VerticleLogger

/**
 * WorkerHealthChangeEvent handles status messages from FrameWorkerHealthMonitorVerticle
 */
class WorkerHealthChangeEvent @Inject constructor(
    private val vlog: VerticleLogger
) {
    suspend fun register() {
        // TODO: Monitor worker health changes
        // Future implementation will listen to:
        // - Heartbeat timeout events
        // - Worker disconnection events
        // - Health check failure events
        vlog.logInfo("WorkerHealthChangeEvent registered (stub implementation)")
    }
}