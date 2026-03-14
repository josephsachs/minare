package com.minare.core.frames.coordinator.services

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.application.config.FrameworkConfig
import com.minare.core.frames.services.VerticleRegistry
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
    private val verticleRegistry: VerticleRegistry,
    private val frameworkConfig: FrameworkConfig,
    private val affinityResolver: AffinityResolver,
    private val debug: DebugLogger
) {
    private val manifestMap: IMap<String, JsonObject> by lazy {
        hazelcastInstance.getMap("frame-manifests")
    }

    private val operationOrder: Comparator<JsonObject> =
        compareBy<JsonObject> { it.getString("operationSetId") ?: it.getString("id") }
            .thenBy { it.getInteger("setIndex") ?: 0 }

    /**
     * Distribute operations among workers.
     *
     * When affinity scopes are configured, delegates to AffinityResolver for
     * relationship-aware routing (single Redis batch read per frame).
     *
     * When no scopes are configured, falls back to consistent hashing by routing key
     * (operationSetId for set members, operationId for solo operations). Zero-cost path:
     * no Redis calls, no reflection.
     *
     * @param operations The operations to distribute
     * @param workers The available workers
     * @return Map of worker ID to assigned operations
     */
    suspend fun distribute(
        operations: List<JsonObject>,
        workers: Set<String>
    ): Map<String, List<JsonObject>> {
        if (workers.isEmpty()) return emptyMap()

        // Delegate to AffinityResolver when scopes are configured
        if (frameworkConfig.frames.groupOperationsBy.isNotEmpty()) {
            return affinityResolver.resolve(operations, workers)
        }

        // Zero-cost fallback: consistent hashing by routing key
        val workerList = workers.toList().sorted()

        return operations.groupBy { op ->
            val routingKey = op.getString("operationSetId") ?: op.getString("id")
            val hash = abs(routingKey.hashCode())
            workerList[hash % workerList.size]
        }
    }

    /**
     * Write manifests to Hazelcast distributed map.
     * Each worker gets a manifest even if it has no operations assigned.
     *
     * Ordering rule:
     * - Primary key: operationSetId (for set members) or operationId (for solo ops).
     *   UUID-based keys give fair random interleaving; shared operationSetId
     *   groups set members contiguously.
     * - Secondary key: setIndex preserves declaration order within a set.
     *
     * @param logicalFrame The logical frame number (not wall clock!)
     * @param assignments Map of worker ID to assigned operations
     * @param activeWorkers Set of all active workers
     */
    fun writeManifests(
        logicalFrame: Long,
        assignments: Map<String, List<JsonObject>>,
        activeWorkers: Set<String>
    ) {
        activeWorkers.forEach { workerId ->
            val operations = assignments[workerId] ?: emptyList()

            val sortedOperations = operations.sortedWith(operationOrder)

            val manifest = JsonObject()
                .put("workerId", workerId)
                .put("logicalFrame", logicalFrame)
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
     * we have already prepared but not begun processing.
     *
     * Uses operationSetId as routing key for set members so late-arriving
     * set operations land on the same worker as their siblings.
     */
    fun assignToExistingManifest(operation: JsonObject, frame: Long) {
        try {
            val activeWorkers = verticleRegistry.getActiveInstances()

            val routingKey = operation.getString("operationSetId") ?: operation.getString("id")
            val workerIndex = abs(routingKey.hashCode()) % activeWorkers.size
            val workerId = activeWorkers.toList()[workerIndex]
            val manifestKey = FrameManifest.makeKey(frame, workerId)

            val manifestJson = manifestMap[manifestKey]

            if (manifestJson != null) {
                val manifest = FrameManifest.fromJson(manifestJson)
                val operations = manifest.operations.toMutableList()
                operations.add(operation)
                operations.sortWith(operationOrder)

                val updatedManifest = FrameManifest(
                    workerId = manifest.workerId,
                    logicalFrame = manifest.logicalFrame,
                    createdAt = manifest.createdAt,
                    operations = operations
                )

                manifestMap[manifestKey] = updatedManifest.toJson()

                debug.log(DebugType.COORDINATOR_MANIFEST_BUILDER_ASSIGNED_OPERATIONS, listOf(routingKey, frame))
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