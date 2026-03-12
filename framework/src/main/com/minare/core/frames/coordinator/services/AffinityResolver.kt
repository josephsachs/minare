package com.minare.core.frames.coordinator.services

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.application.config.FrameworkConfig
import com.minare.core.entity.annotations.Child
import com.minare.core.entity.annotations.Parent
import com.minare.core.entity.annotations.Peer
import com.minare.core.entity.services.EntityInspector
import com.minare.core.frames.coordinator.models.AffinityScopeType
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import kotlin.math.abs

/**
 * Resolves operation-to-worker affinity based on configured AffinityScopeTypes.
 *
 * Builds a per-frame affinity map (entityId → workerId) using first-come-first-served
 * semantics. Each configured scope type expands the set of entity IDs that should
 * co-locate on the same worker.
 *
 * Performs a single batch Redis read per frame regardless of operation count.
 */
@Singleton
class AffinityResolver @Inject constructor(
    private val frameworkConfig: FrameworkConfig,
    private val stateStore: StateStore,
    private val entityInspector: EntityInspector,
    private val debug: DebugLogger
) {
    private val log = LoggerFactory.getLogger(AffinityResolver::class.java)

    /**
     * Resolve operation-to-worker assignments using affinity scopes.
     *
     * @param operations All operations for a single logical frame
     * @param workers Available worker IDs
     * @return Map of workerId to assigned operations
     */
    suspend fun resolve(
        operations: List<JsonObject>,
        workers: Set<String>
    ): Map<String, List<JsonObject>> {
        if (workers.isEmpty()) return emptyMap()
        if (operations.isEmpty()) return emptyMap()

        val workerList = workers.toList().sorted()
        val scopes = frameworkConfig.frames.groupOperationsBy
        val affinityMap = mutableMapOf<String, String>() // entityId → workerId
        val assignments = mutableMapOf<String, MutableList<JsonObject>>()

        // Initialize empty lists for all workers
        workerList.forEach { assignments[it] = mutableListOf() }

        // Single batch read: collect all entityIds from all operations
        val allEntityIds = operations.mapNotNull { it.getString("entityId") }.distinct()
        val entityCache: Map<String, JsonObject> = if (allEntityIds.isNotEmpty()) {
            stateStore.findJson(allEntityIds)
        } else {
            emptyMap()
        }

        // Group operations into work units (set members grouped, solo ops individual)
        val workUnits = operations.groupBy { op ->
            op.getString("operationSetId") ?: op.getString("id")
        }

        for ((routingKey, workUnitOps) in workUnits) {
            // Build the cohort: all entity IDs that should co-locate
            val cohort = mutableSetOf<String>()

            // Always include the operation's own entityId(s)
            workUnitOps.mapNotNullTo(cohort) { it.getString("entityId") }

            // TARGETS: scan delta values for entity IDs present in the store
            if (AffinityScopeType.TARGETS in scopes) {
                val targetIds = extractTargetIds(workUnitOps, entityCache)
                cohort.addAll(targetIds)
            }

            // FIELD_PARENT / FIELD_PEER / FIELD_CHILD: discover relationship fields,
            // extract referenced entity IDs for each configured annotation type.
            val fieldAnnotations = listOfNotNull(
                Parent::class.takeIf { AffinityScopeType.FIELD_PARENT in scopes },
                Peer::class.takeIf { AffinityScopeType.FIELD_PEER in scopes },
                Child::class.takeIf { AffinityScopeType.FIELD_CHILD in scopes }
            )
            if (fieldAnnotations.isNotEmpty()) {
                cohort.addAll(relatedFieldIds(cohort, entityCache, fieldAnnotations))
            }

            // First mapped entity in cohort wins — follow that worker
            val existingWorker = cohort.firstNotNullOfOrNull { affinityMap[it] }
            val worker = existingWorker ?: hashToWorker(routingKey, workerList)

            // Claim all cohort members under this worker
            cohort.forEach { affinityMap[it] = worker }

            // Assign all operations in this work unit to the worker
            assignments.getOrPut(worker) { mutableListOf() }.addAll(workUnitOps)
        }

        debug.log(DebugType.COORDINATOR_AFFINITY_RESOLVED, listOf(
            operations.size,
            affinityMap.size,
            workUnits.size
        ))

        return assignments
    }

    /**
     * Extract entity IDs from operation deltas that correspond to real entities.
     * Walks each delta's values, keeping only strings that resolve to entities
     * in the batch cache.
     */
    private fun extractTargetIds(
        operations: List<JsonObject>,
        entityCache: Map<String, JsonObject>
    ): Set<String> {
        val targets = mutableSetOf<String>()

        for (op in operations) {
            val delta = (op.getValue("delta") as? JsonObject) ?: continue
            for (key in delta.fieldNames()) {
                val value = delta.getValue(key)
                if (value is String && value.isNotBlank() && entityCache.containsKey(value)) {
                    targets.add(value)
                }
            }
        }

        return targets
    }

    /**
     * Returns entity IDs referenced by relationship fields of the given annotation types.
     * Uses EntityInspector for field discovery and extracts IDs from the entity's state
     * in the batch cache.
     */
    private suspend fun relatedFieldIds(
        cohort: Set<String>,
        entityCache: Map<String, JsonObject>,
        annotationTypes: List<kotlin.reflect.KClass<out Annotation>>
    ): Set<String> {
        val related = mutableSetOf<String>()

        for (entityId in cohort) {
            val entityJson = entityCache[entityId] ?: continue
            val state = entityJson.getJsonObject("state") ?: continue

            val relationshipFields = entityInspector.getFieldsOfType(
                entityId,
                annotationTypes
            )

            for (field in relationshipFields) {
                val fieldValue = state.getValue(field.name) ?: continue
                extractEntityIds(fieldValue).forEach { related.add(it) }
            }
        }

        return related
    }

    /**
     * Extract entity IDs from a field value.
     * Handles String, List<String>, and JsonArray — the three forms
     * relationship fields can take.
     */
    private fun extractEntityIds(value: Any?): List<String> {
        return when (value) {
            null -> emptyList()
            is String -> if (value.isNotBlank()) listOf(value) else emptyList()
            is List<*> -> value.filterIsInstance<String>()
            is JsonArray -> value.list.filterIsInstance<String>()
            else -> emptyList()
        }
    }

    /**
     * Hash a routing key to a worker using consistent hashing.
     */
    private fun hashToWorker(routingKey: String, workerList: List<String>): String {
        val hash = abs(routingKey.hashCode())
        return workerList[hash % workerList.size]
    }
}
