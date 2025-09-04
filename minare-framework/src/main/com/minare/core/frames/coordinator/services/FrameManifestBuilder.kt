package com.minare.core.frames.coordinator.services

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.core.utils.debug.OperationDebugUtils
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.math.abs

/**
 * Handles the creation and distribution of frame manifests.
 * Responsible for dividing operations among workers and writing
 * manifests to Hazelcast for worker consumption.
 */
@Singleton
class FrameManifestBuilder @Inject constructor(
    private val hazelcastInstance: HazelcastInstance,
    private val operationDebugUtils: OperationDebugUtils
) {
    private val log = LoggerFactory.getLogger(FrameManifestBuilder::class.java)

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
        if (workers.isEmpty()) {
            log.debug("No workers available for operation distribution")
            return emptyMap()
        }

        // Sort workers for consistent hashing
        val workerList = workers.toList().sorted()

        return operations.groupBy { op ->
            // TEMPORARY DEBUG
            operationDebugUtils.logOperation(op, "FrameManifestBuilder: distributeOperations")

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

            log.debug("Wrote manifest for worker {} with {} operations for logical frame {}",
                workerId, sortedOperations.size, logicalFrame)
        }

        log.debug("Created manifests for logical frame {} with {} total operations distributed to {} workers",
            logicalFrame, assignments.values.sumOf { it.size }, activeWorkers.size)
    }

    /**
     * Clear manifests for a completed frame.
     * Should be called after frame completion to free memory.
     */
    fun clearFrameManifests(logicalFrame: Long) {
        val manifestKeys = manifestMap.keys
            .filter { it.startsWith("manifest:$logicalFrame:") }

        manifestKeys.forEach { manifestMap.remove(it) }

        log.debug("Cleared {} manifests for frame {}", manifestKeys.size, logicalFrame)
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
            log.debug("Cleared {} manifests from distributed map for new session", keysToRemove.size)
        } else {
            log.debug("No manifests to clear for new session")
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