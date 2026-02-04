package com.minare.core.frames.coordinator.services

import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType
import com.minare.worker.coordinator.events.WorkerReadinessEvent
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.delay
import com.google.inject.Inject
import com.google.inject.Singleton

/**
 * Minimal service to ensure all workers are ready before starting session.
 * Fixes the manifest polling race condition.
 */
@Singleton
class StartupService @Inject constructor(
    private val workerRegistry: WorkerRegistry
) {
    private val debug = DebugLogger()
    private val workersReady = CompletableDeferred<Unit>()

    /**
     * Check if all workers are ready at coordinator startup.
     */
    suspend fun checkInitialWorkerStatus() {
        // This is an ugly hack to fix a race condition where the coordinator starts before the infrastructure
        // stack has set the expected workers. We should find the "correct" way to order this.
        while (workerRegistry.getExpectedWorkerCount() == 0) {
            debug.log(DebugType.COORDINATOR_STARTUP_WAITING_FOR_INFRA_POST)
            delay(500)
        }

        val activeCount = workerRegistry.getActiveWorkers().size
        val expectedCount = workerRegistry.getExpectedWorkerCount()

        debug.log(DebugType.COORDINATOR_STARTUP_INITIAL_WORKER_STATUS, listOf(activeCount, expectedCount))

        if (activeCount == expectedCount && expectedCount > 0) {
            debug.log(DebugType.COORDINATOR_STARTUP_ALL_WORKERS_ALREADY_HERE)

            workersReady.complete(Unit)
        }
    }

    /**
     * Handle a worker becoming ready.
     */
    fun handleWorkerReady(workerId: String) {
        val activeCount = workerRegistry.getActiveWorkers().size
        val expectedCount = workerRegistry.getExpectedWorkerCount()

        debug.log(DebugType.COORDINATOR_STARTUP_HANDLE_WORKER_READY, listOf(workerId, activeCount, expectedCount))

        if (activeCount == expectedCount && !workersReady.isCompleted) {
            debug.log(DebugType.COORDINATOR_STARTUP_ALL_WORKERS_READY)

            workersReady.complete(Unit)
        }
    }

    /**
     * Wait for all workers to be ready.
     */
    suspend fun awaitAllWorkersReady(event: WorkerReadinessEvent) {
        event.register()
        workersReady.await()
    }
}