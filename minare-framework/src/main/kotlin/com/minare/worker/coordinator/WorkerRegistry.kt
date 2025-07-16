package com.minare.coordinator

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
     * Handle worker registration - transition from PENDING to ACTIVE
     */
    fun registerWorker(workerId: String): Boolean {
        val state = workers[workerId]

        return when {
            state == null -> {
                log.warn("Unknown worker {} attempted registration", workerId)
                false
            }
            state.status == WorkerStatus.PENDING -> {
                log.info("Worker {} registered successfully", workerId)
                workers[workerId] = state.copy(
                    status = WorkerStatus.ACTIVE,
                    lastHeartbeat = System.currentTimeMillis()
                )
                true
            }
            else -> {
                log.warn("Worker {} attempted registration in state {}", workerId, state.status)
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
        workers[workerId]?.completedFrames?.add(frameStartTime)
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
                    workers.remove(workerId)
                    log.info("Removed worker {} from registry", workerId)
                }
                else -> {}
            }
        }
    }

    /**
     * Get all active workers
     */
    fun getActiveWorkers(): Set<String> {
        return workers.values
            .filter { it.status == WorkerStatus.ACTIVE }
            .map { it.workerId }
            .toSet()
    }

    /**
     * Get worker state
     */
    fun getWorkerState(workerId: String): WorkerState? {
        return workers[workerId]
    }

    /**
     * Mark workers as unhealthy
     */
    fun markWorkersUnhealthy(workerIds: Collection<String>) {
        workerIds.forEach { workerId ->
            workers[workerId]?.let { state ->
                workers[workerId] = state.copy(status = WorkerStatus.UNHEALTHY)
            }
        }
    }

    /**
     * Check if minimum number of workers are available
     */
    fun hasMinimumWorkers(minimum: Int = 1): Boolean {
        return getActiveWorkers().size >= minimum
    }

    /**
     * Get all workers with their states (for monitoring)
     */
    fun getAllWorkers(): Map<String, WorkerState> {
        return workers.toMap()
    }
}