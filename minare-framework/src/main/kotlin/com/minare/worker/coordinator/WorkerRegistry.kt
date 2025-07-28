package com.minare.worker.coordinator

import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Manages the authoritative registry of workers in the cluster.
 * This is the single source of truth for which workers exist and their states.
 *
 * Updated to use Hazelcast distributed map instead of local storage.
 */
@Singleton
class WorkerRegistry @Inject constructor(
    private val workerRegistryMap: WorkerRegistryMap
) {
    private val log = LoggerFactory.getLogger(WorkerRegistry::class.java)

    data class WorkerState(
        val workerId: String,
        val status: WorkerStatus,
        val lastHeartbeat: Long = System.currentTimeMillis(),
        val addedAt: Long = System.currentTimeMillis()
        // completedFrames removed - not needed and saves distributed map traffic
    ) {
        /**
         * Convert to JsonObject for distributed map storage
         */
        fun toJson(): JsonObject {
            return JsonObject()
                .put("workerId", workerId)
                .put("status", status.name)
                .put("lastHeartbeat", lastHeartbeat)
                .put("addedAt", addedAt)
        }

        companion object {
            /**
             * Create WorkerState from JsonObject
             */
            fun fromJson(json: JsonObject): WorkerState {
                return WorkerState(
                    workerId = json.getString("workerId"),
                    status = WorkerStatus.valueOf(json.getString("status")),
                    lastHeartbeat = json.getLong("lastHeartbeat"),
                    addedAt = json.getLong("addedAt")
                )
            }
        }
    }

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
        val state = WorkerState(
            workerId = workerId,
            status = WorkerStatus.PENDING
        )
        workerRegistryMap.put(workerId, state.toJson())
    }

    /**
     * Schedule a worker for removal
     */
    fun scheduleWorkerRemoval(workerId: String) {
        log.info("Scheduling removal of worker {}", workerId)
        val json = workerRegistryMap.get(workerId)
        if (json != null) {
            val state = WorkerState.fromJson(json)
            workerRegistryMap.put(workerId, state.copy(status = WorkerStatus.REMOVING).toJson())
        }
    }

    /**
     * Activate a worker that has completed startup.
     * Transitions from PENDING to ACTIVE state.
     *
     * @return true if activation was successful, false otherwise
     */
    fun activateWorker(workerId: String): Boolean {
        val json = workerRegistryMap.get(workerId)
        val state = json?.let { WorkerState.fromJson(it) }

        return when {
            state == null -> {
                log.warn("Unknown worker {} attempted activation", workerId)
                false
            }
            state.status == WorkerStatus.PENDING -> {
                log.info("Worker {} activated successfully", workerId)
                workerRegistryMap.put(workerId, state.copy(
                    status = WorkerStatus.ACTIVE,
                    lastHeartbeat = System.currentTimeMillis()
                ).toJson())
                true
            }
            state.status == WorkerStatus.ACTIVE -> {
                log.debug("Worker {} already active, updating heartbeat", workerId)
                workerRegistryMap.put(workerId, state.copy(
                    lastHeartbeat = System.currentTimeMillis()
                ).toJson())
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
        val json = workerRegistryMap.get(workerId)
        if (json != null) {
            val state = WorkerState.fromJson(json)
            val updatedState = state.copy(
                lastHeartbeat = System.currentTimeMillis(),
                status = if (state.status == WorkerStatus.UNHEALTHY) {
                    log.info("Worker {} recovered from unhealthy state", workerId)
                    WorkerStatus.ACTIVE
                } else {
                    state.status
                }
            )
            workerRegistryMap.put(workerId, updatedState.toJson())
        }
    }

    /**
     * Record frame completion for a worker
     * Note: completedFrames tracking removed to reduce distributed map traffic
     */
    fun recordFrameCompletion(workerId: String, frameStartTime: Long) {
        // Just update heartbeat to prove liveness
        updateHeartbeat(workerId)
    }

    /**
     * Update health status of all workers based on heartbeat timeout
     */
    fun updateWorkerHealth(heartbeatTimeout: Long) {
        val now = System.currentTimeMillis()

        // Note: This iterates over distributed map entries
        workerRegistryMap.entries().forEach { entry ->
            val workerId = entry.key
            val state = WorkerState.fromJson(entry.value)

            when (state.status) {
                WorkerStatus.ACTIVE -> {
                    if (now - state.lastHeartbeat > heartbeatTimeout) {
                        log.warn("Worker {} is unhealthy (last heartbeat: {}ms ago)",
                            workerId, now - state.lastHeartbeat)
                        workerRegistryMap.put(workerId, state.copy(status = WorkerStatus.UNHEALTHY).toJson())
                    }
                }
                WorkerStatus.REMOVING -> {
                    // Actually remove workers marked for removal
                    workerRegistryMap.remove(workerId)
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
            val json = workerRegistryMap.get(workerId)
            if (json != null) {
                val state = WorkerState.fromJson(json)
                workerRegistryMap.put(workerId, state.copy(status = WorkerStatus.UNHEALTHY).toJson())
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
        return workerRegistryMap.entries()
            .map { WorkerState.fromJson(it.value) }
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
        val json = workerRegistryMap.get(workerId)
        return json?.let { WorkerState.fromJson(it) }
    }

    /**
     * Get all workers and their states (for monitoring)
     */
    fun getAllWorkers(): Map<String, WorkerState> {
        return workerRegistryMap.entries().associate {
            it.key to WorkerState.fromJson(it.value)
        }
    }

    /**
     * Check if a specific worker is healthy and active
     */
    fun isWorkerHealthy(workerId: String): Boolean {
        val json = workerRegistryMap.get(workerId)
        return json?.let { WorkerState.fromJson(it).status == WorkerStatus.ACTIVE } ?: false
    }

    /**
     * Get count of workers by status (for monitoring)
     */
    fun getWorkerCountByStatus(): Map<WorkerStatus, Int> {
        return workerRegistryMap.values()
            .map { WorkerState.fromJson(it) }
            .groupBy { it.status }
            .mapValues { it.value.size }
    }

    /**
     * Remove a worker immediately (used in testing or emergency scenarios)
     */
    fun removeWorkerImmediately(workerId: String): Boolean {
        val removed = workerRegistryMap.remove(workerId)
        if (removed != null) {
            log.warn("Worker {} removed immediately from registry", workerId)
            return true
        }
        return false
    }

    /**
     * Clear all completed frame records (used between frames to save memory)
     * Note: This method is now a no-op since we removed completedFrames tracking
     */
    fun clearFrameCompletionHistory() {
        // No-op - completedFrames removed
    }

    /**
     * Reset the registry (useful for testing)
     */
    fun reset() {
        workerRegistryMap.clear()
        log.info("Worker registry reset")
    }
}