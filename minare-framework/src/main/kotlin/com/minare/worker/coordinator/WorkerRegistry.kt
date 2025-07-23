package com.minare.worker.coordinator

import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Singleton

/**
 * Manages the authoritative registry of workers in the cluster.
 * This is the single source of truth for which workers exist and their states.
 */
@Singleton
class WorkerRegistry {
    private val log = LoggerFactory.getLogger(WorkerRegistry::class.java)

    // The authoritative worker registry
    private val workers = ConcurrentHashMap<String, WorkerState>()

    data class WorkerState(
        val workerId: String,
        val status: WorkerStatus,
        val lastHeartbeat: Long = System.currentTimeMillis(),
        val addedAt: Long = System.currentTimeMillis(),
        val completedFrames: MutableSet<Long> = ConcurrentHashMap.newKeySet()
    )

    enum class WorkerStatus {
        PENDING,      // Added but not yet active
        ACTIVE,       // Participating in frames
        UNHEALTHY,    // Missed heartbeats
        REMOVING      // Scheduled for removal
    }

    /**
     * Add a new worker to the registry in PENDING state
     */
    fun addWorker(workerId: String) {
        log.info("Adding worker {} to registry", workerId)
        workers[workerId] = WorkerState(
            workerId = workerId,
            status = WorkerStatus.PENDING
        )
    }

    /**
     * Schedule a worker for removal
     */
    fun scheduleWorkerRemoval(workerId: String) {
        log.info("Scheduling removal of worker {}", workerId)
        workers[workerId]?.let {
            workers[workerId] = it.copy(status = WorkerStatus.REMOVING)
        }
    }

    /**
     * Activate a worker that has completed startup.
     * Transitions from PENDING to ACTIVE state.
     *
     * @return true if activation was successful, false otherwise
     */
    fun activateWorker(workerId: String): Boolean {
        val state = workers[workerId]

        return when {
            state == null -> {
                log.warn("Unknown worker {} attempted activation", workerId)
                false
            }
            state.status == WorkerStatus.PENDING -> {
                log.info("Worker {} activated successfully", workerId)
                workers[workerId] = state.copy(
                    status = WorkerStatus.ACTIVE,
                    lastHeartbeat = System.currentTimeMillis()
                )
                true
            }
            state.status == WorkerStatus.ACTIVE -> {
                log.debug("Worker {} already active, updating heartbeat", workerId)
                workers[workerId] = state.copy(
                    lastHeartbeat = System.currentTimeMillis()
                )
                true
            }
            else -> {
                log.warn("Worker {} attempted activation in state {}", workerId, state.status)
                false
            }
        }
    }

    /**
     * Update worker heartbeat
     */
    fun updateHeartbeat(workerId: String) {
        workers[workerId]?.let { state ->
            workers[workerId] = state.copy(
                lastHeartbeat = System.currentTimeMillis(),
                status = if (state.status == WorkerStatus.UNHEALTHY) {
                    log.info("Worker {} recovered from unhealthy state", workerId)
                    WorkerStatus.ACTIVE
                } else {
                    state.status
                }
            )
        }
    }

    /**
     * Record frame completion for a worker
     */
    fun recordFrameCompletion(workerId: String, frameStartTime: Long) {
        workers[workerId]?.let { state ->
            state.completedFrames.add(frameStartTime)

            // Also update heartbeat since completing a frame proves liveness
            workers[workerId] = state.copy(
                lastHeartbeat = System.currentTimeMillis()
            )
        }
    }

    /**
     * Update health status of all workers based on heartbeat timeout
     */
    fun updateWorkerHealth(heartbeatTimeout: Long) {
        val now = System.currentTimeMillis()

        workers.forEach { (workerId, state) ->
            when (state.status) {
                WorkerStatus.ACTIVE -> {
                    if (now - state.lastHeartbeat > heartbeatTimeout) {
                        log.warn("Worker {} is unhealthy (last heartbeat: {}ms ago)",
                            workerId, now - state.lastHeartbeat)
                        workers[workerId] = state.copy(status = WorkerStatus.UNHEALTHY)
                    }
                }
                WorkerStatus.REMOVING -> {
                    // Actually remove workers marked for removal
                    workers.remove(workerId)
                    log.info("Removed worker {} from registry", workerId)
                }
                else -> {}
            }
        }
    }

    /**
     * Mark specific workers as unhealthy
     */
    fun markWorkersUnhealthy(workerIds: Collection<String>) {
        workerIds.forEach { workerId ->
            workers[workerId]?.let { state ->
                workers[workerId] = state.copy(status = WorkerStatus.UNHEALTHY)
                log.warn("Marked worker {} as unhealthy", workerId)
            }
        }
    }

    /**
     * Get all active workers.
     * This is the key method for frame coordination - only ACTIVE workers
     * should receive frame manifests and be expected to complete frames.
     */
    fun getActiveWorkers(): Set<String> {
        return workers.values
            .filter { it.status == WorkerStatus.ACTIVE }
            .map { it.workerId }
            .toSet()
    }

    /**
     * Get workers that were active at frame start but may have failed.
     * Used for recovery scenarios.
     */
    fun getWorkersActiveAtFrame(frameStartTime: Long): Set<String> {
        // For now, return current active workers
        // Could be enhanced to track historical state if needed
        return getActiveWorkers()
    }

    /**
     * Check if we have the minimum number of workers to process
     */
    fun hasMinimumWorkers(): Boolean {
        val activeCount = getActiveWorkers().size
        val minimumWorkers = System.getenv("MIN_WORKERS")?.toIntOrNull() ?: 1
        return activeCount >= minimumWorkers
    }

    /**
     * Get worker state
     */
    fun getWorkerState(workerId: String): WorkerState? {
        return workers[workerId]
    }

    /**
     * Get all workers and their states (for monitoring)
     */
    fun getAllWorkers(): Map<String, WorkerState> {
        return workers.toMap()
    }

    /**
     * Check if a specific worker is healthy and active
     */
    fun isWorkerHealthy(workerId: String): Boolean {
        return workers[workerId]?.status == WorkerStatus.ACTIVE
    }

    /**
     * Get count of workers by status (for monitoring)
     */
    fun getWorkerCountByStatus(): Map<WorkerStatus, Int> {
        return workers.values
            .groupBy { it.status }
            .mapValues { it.value.size }
    }

    /**
     * Remove a worker immediately (used in testing or emergency scenarios)
     */
    fun removeWorkerImmediately(workerId: String): Boolean {
        val removed = workers.remove(workerId)
        if (removed != null) {
            log.warn("Worker {} removed immediately from registry", workerId)
            return true
        }
        return false
    }

    /**
     * Clear all completed frame records (used between frames to save memory)
     */
    fun clearFrameCompletionHistory() {
        workers.values.forEach { state ->
            state.completedFrames.clear()
        }
    }

    /**
     * Reset the registry (useful for testing)
     */
    fun reset() {
        workers.clear()
        log.info("Worker registry reset")
    }
}