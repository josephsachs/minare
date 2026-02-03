package com.minare.core.frames.coordinator.services

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.utils.debug.DebugLogger
import com.minare.exceptions.FrameLoopException
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType
import com.minare.worker.coordinator.models.FrameManifest
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import com.google.inject.Inject
import com.google.inject.Singleton
import kotlin.math.abs

/**
 * Handles the creation and distribution of frame manifests.
 * Responsible for dividing operations among workers and writing
 * manifests to Hazelcast for worker consumption.
 */
@Singleton
class FrameManifestBuilder @Inject constructor(
    private val hazelcastInstance: HazelcastInstance,
    private val workerRegistry: WorkerRegistry,
    private val debug: DebugLogger
) {
    private val manifestMap: IMap<String, JsonObject> by lazy {
        hazelcastInstance.getMap("frame-manifests")
    }

    /**
     * Distribute operations among workers using consistent hashing.
     * Operations are distributed by operation ID to ensure domain agnosticism.
     *
     * @param operations The operations to distribute
     * @param workers The available workers
     * @return Map of worker ID to assigned operations
     */
    fun distributeOperations(
        operations: List<JsonObject>,
        workers: Set<String>
    ): Map<String, List<JsonObject>> {
        if (workers.isEmpty()) return emptyMap()

        // Sort workers for consistent hashing
        val workerList = workers.toList().sorted()

        return operations.groupBy { op ->
            // Use operation ID for consistent hashing
            // This keeps the operation pipeline entity-agnostic
            val operationId = op.getString("id")

            val hash = abs(operationId.hashCode())
            workerList[hash % workerList.size]
        }
    }

    /**
     * Write manifests to Hazelcast distributed map.
     * Each worker gets a manifest even if it has no operations assigned.
     *
     * UPDATED: Operations are now sorted by operation ID hash for deterministic
     * ordering that doesn't favor any particular producer.
     *
     * @param logicalFrame The logical frame number (not wall clock!)
     * @param assignments Map of worker ID to assigned operations
     * @param activeWorkers Set of all active workers
     */
    fun writeManifestsToMap(
        logicalFrame: Long,  // Now this is logical frame number
        assignments: Map<String, List<JsonObject>>,
        activeWorkers: Set<String>
    ) {
        activeWorkers.forEach { workerId ->
            val operations = assignments[workerId] ?: emptyList()

            val sortedOperations = operations.sortedBy { op ->
                op.getString("id")
            }

            val manifest = JsonObject()
                .put("workerId", workerId)
                .put("logicalFrame", logicalFrame)  // Use logical frame
                .put("operations", JsonArray(sortedOperations))
                .put("createdAt", System.currentTimeMillis())

            val key = "manifest:$logicalFrame:$workerId"
            manifestMap[key] = manifest

            debug.log(
                DebugType.COORDINATOR_MANIFEST_BUILDER_WROTE_WORKER,
                listOf(workerId, sortedOperations.size, logicalFrame)
            )
        }

        debug.log(
            DebugType.COORDINATOR_MANIFEST_BUILDER_WROTE_ALL, listOf(
                logicalFrame,
                assignments.values.sumOf { it.size },
                activeWorkers.size
            )
        )
    }


    /**
     * Handle assignment of operations to prior manifests, ex. if they belong in a logical frame
     * we have already prepared but not begun processing
     */
    fun assignToExistingManifest(operation: JsonObject, frame: Long) {
        try {
            val manifestMap = hazelcastInstance.getMap<String, JsonObject>("frame-manifests")
            val activeWorkers = workerRegistry.getActiveWorkers()

            val operationId = operation.getString("id")
            val workerIndex = Math.abs(operationId.hashCode()) % activeWorkers.size
            val workerId = activeWorkers.toList()[workerIndex]
            val manifestKey = FrameManifest.makeKey(frame, workerId) // Note: targetFrame!

            val manifestJson = manifestMap[manifestKey]

            if (manifestJson != null) {
                val manifest = FrameManifest.fromJson(manifestJson)
                val operations = manifest.operations.toMutableList()
                operations.add(operation)
                operations.sortBy { it.getString("id") }

                val updatedManifest = FrameManifest(
                    workerId = manifest.workerId,
                    logicalFrame = manifest.logicalFrame,
                    createdAt = manifest.createdAt,
                    operations = operations
                )

                manifestMap[manifestKey] = updatedManifest.toJson()

                debug.log(DebugType.COORDINATOR_MANIFEST_BUILDER_ASSIGNED_OPERATIONS, listOf(operationId, frame))
            }
        } catch (e: Exception) {
            throw FrameLoopException("Buffered operation assignment: Error updating manifest ${e}")
        }
    }

    /**
     * Clear manifests for a completed frame.
     * Should be called after frame completion to free memory.
     */
    fun clearFrameManifests(logicalFrame: Long) {
        val manifestKeys = manifestMap.keys
            .filter { it.startsWith("manifest:$logicalFrame:") }

        manifestKeys.forEach { manifestMap.remove(it) }

        debug.log(DebugType.COORDINATOR_MANIFEST_BUILDER_CLEAR_FRAMES, listOf(manifestKeys.size, logicalFrame))
    }

    /**
     * Get manifest for a specific worker and frame.
     * Used by workers to fetch their assigned operations.
     */
    fun getManifest(logicalFrame: Long, workerId: String): JsonObject? {
        val key = "manifest:$logicalFrame:$workerId"
        return manifestMap[key]
    }

    /**
     * Clear ALL manifests from the distributed map.
     * Should be called when starting a new session to ensure clean state.
     */
    fun clearAllManifests() {
        val keysToRemove = manifestMap.keys.filter { it.startsWith("manifest:") }

        if (keysToRemove.isNotEmpty()) {
            keysToRemove.forEach { manifestMap.remove(it) }
            debug.log(DebugType.COORDINATOR_MANIFEST_BUILDER_CLEAR_ALL, listOf(keysToRemove.size))
        }
    }

    /**
     * Get all manifests for a frame.
     * Useful for monitoring and debugging.
     */
    fun getFrameManifests(logicalFrame: Long): Map<String, JsonObject> {
        return manifestMap.entries
            .filter { it.key.startsWith("manifest:$logicalFrame:") }
            .associate {
                val workerId = it.key.substringAfterLast(":")
                workerId to it.value
            }
    }
}