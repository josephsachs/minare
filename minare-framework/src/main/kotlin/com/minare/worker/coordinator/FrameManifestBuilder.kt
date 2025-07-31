package com.minare.worker.coordinator

import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
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
    private val hazelcastInstance: HazelcastInstance
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
            // Use operation ID for consistent hashing
            // This keeps the operation pipeline entity-agnostic
            val operationId = op.getString("id") ?: ""

            val hash = abs(operationId.hashCode())
            workerList[hash % workerList.size]
        }
    }

    /**
     * Write manifests to Hazelcast distributed map.
     * Each worker gets a manifest even if it has no operations assigned.
     *
     * @param frameStartTime The start time of the frame
     * @param frameEndTime The end time of the frame
     * @param assignments Map of worker ID to assigned operations
     * @param activeWorkers Set of all active workers
     */
    fun writeManifestsToMap(
        frameStartTime: Long,
        frameEndTime: Long,
        assignments: Map<String, List<JsonObject>>,
        activeWorkers: Set<String>
    ) {

        // Write manifest for each worker (empty if no operations assigned)
        activeWorkers.forEach { workerId ->
            val operations = assignments[workerId] ?: emptyList()

            val manifest = JsonObject()
                .put("workerId", workerId)
                .put("frameStartTime", frameStartTime)
                .put("frameEndTime", frameEndTime)
                .put("operations", JsonArray(operations))
                .put("createdAt", System.currentTimeMillis())

            val key = "manifest:$frameStartTime:$workerId"
            manifestMap[key] = manifest

            log.trace("Wrote manifest for worker {} with {} operations",
                workerId, operations.size)
        }

        log.debug("Created manifests for frame {} with {} total operations distributed to {} workers",
            frameStartTime, assignments.values.sumOf { it.size }, activeWorkers.size)
    }

    /**
     * Clear manifests for a completed frame.
     * Should be called after frame completion to free memory.
     */
    fun clearFrameManifests(frameStartTime: Long) {
        val manifestKeys = manifestMap.keys
            .filter { it.startsWith("manifest:$frameStartTime:") }

        manifestKeys.forEach { manifestMap.remove(it) }

        log.debug("Cleared {} manifests for frame {}", manifestKeys.size, frameStartTime)
    }

    /**
     * Get manifest for a specific worker and frame.
     * Used by workers to fetch their assigned operations.
     */
    fun getManifest(frameStartTime: Long, workerId: String): JsonObject? {
        val key = "manifest:$frameStartTime:$workerId"
        return manifestMap[key]
    }

    /**
     * Get all manifests for a frame.
     * Useful for monitoring and debugging.
     */
    fun getFrameManifests(frameStartTime: Long): Map<String, JsonObject> {
        return manifestMap.entries
            .filter { it.key.startsWith("manifest:$frameStartTime:") }
            .associate {
                val workerId = it.key.substringAfterLast(":")
                workerId to it.value
            }
    }
}