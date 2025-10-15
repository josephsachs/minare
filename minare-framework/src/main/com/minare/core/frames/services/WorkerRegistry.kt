package com.minare.core.frames.services

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
    private val workerRegistryMap: WorkerRegistryMap,
    private val activeWorkerSet: ActiveWorkerSet
) {
    private val log = LoggerFactory.getLogger(WorkerRegistry::class.java)

    data class WorkerState(
        val workerId: String,
        val status: WorkerStatus,
        val lastHeartbeat: Long = System.currentTimeMillis(),
        val addedAt: Long = System.currentTimeMillis()
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
            activeWorkerSet.remove(workerId)
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
                activeWorkerSet.put(workerId)
                true
            }
            state.status == WorkerStatus.ACTIVE -> {
                log.debug("Worker {} already active, updating heartbeat", workerId)
                workerRegistryMap.put(workerId, state.copy(
                    lastHeartbeat = System.currentTimeMillis()
                ).toJson())
                activeWorkerSet.put(workerId)
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
        val json = workerRegistryMap.get(workerId) ?: return
        val state = WorkerState.fromJson(json)
        workerRegistryMap.put(workerId, state.copy(
            lastHeartbeat = System.currentTimeMillis()
        ).toJson())
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

            if (state.status == WorkerStatus.ACTIVE &&
                (now - state.lastHeartbeat) > heartbeatTimeout) {

                log.warn("Worker {} marked unhealthy (last heartbeat: {}ms ago)",
                    workerId, now - state.lastHeartbeat)

                workerRegistryMap.put(workerId, state.copy(status = WorkerStatus.UNHEALTHY).toJson())
                activeWorkerSet.remove(workerId)
            }
        }
    }

    /**
     * Get list of active workers
     */
    fun getActiveWorkers(): Set<String> {
        return activeWorkerSet.entries()
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
     * Get the expected number of workers.
     * This counts all workers that are not being removed.
     * Used during startup to know when all workers are ready.
     *
     * @return Count of workers in PENDING, ACTIVE, or UNHEALTHY states
     */
    fun getExpectedWorkerCount(): Int {
        return getAllWorkers().count {
            it.value.status != WorkerStatus.REMOVING
        }
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
     * Reset the registry (useful for testing)
     */
    fun reset() {
        workerRegistryMap.clear()
        log.info("Worker registry reset")
    }
}