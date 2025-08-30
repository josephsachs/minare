package com.minare.core.frames.coordinator.services

import com.minare.core.frames.services.WorkerRegistry
import kotlinx.coroutines.CompletableDeferred
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Minimal service to ensure all workers are ready before starting session.
 * Fixes the manifest polling race condition.
 */
@Singleton
class StartupService @Inject constructor(
    private val workerRegistry: WorkerRegistry
) {
    private val log = LoggerFactory.getLogger(StartupService::class.java)

    private val workersReady = CompletableDeferred<Unit>()

    /**
     * Check if all workers are ready at coordinator startup.
     */
    fun checkInitialWorkerStatus() {
        val activeCount = workerRegistry.getActiveWorkers().size
        val expectedCount = workerRegistry.getExpectedWorkerCount()

        log.info("Initial worker check: {}/{} active", activeCount, expectedCount)

        if (activeCount == expectedCount && expectedCount > 0) {
            log.info("All workers already ready")
            workersReady.complete(Unit)
        }
    }

    /**
     * Handle a worker becoming ready.
     */
    fun handleWorkerReady(workerId: String) {
        val activeCount = workerRegistry.getActiveWorkers().size
        val expectedCount = workerRegistry.getExpectedWorkerCount()

        log.info("Worker {} ready: {}/{}", workerId, activeCount, expectedCount)

        if (activeCount == expectedCount && !workersReady.isCompleted) {
            log.info("All workers ready")
            workersReady.complete(Unit)
        }
    }

    /**
     * Wait for all workers to be ready.
     */
    suspend fun awaitAllWorkersReady() {
        workersReady.await()
    }
}