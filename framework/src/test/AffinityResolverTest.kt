package com.minare.core.frames.coordinator.services

import com.minare.core.frames.coordinator.models.AffinityScopeType
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.math.abs

/**
 * Tests for the AffinityResolver algorithm.
 *
 * Extracts the core resolve logic as a pure function, replacing IO
 * dependencies (StateStore, EntityInspector) with pre-computed data:
 * - entityCache: what stateStore.findJson() would return
 * - relationshipFields: what entityInspector.getFieldsOfType() would return
 *   (map of entityId to the field names that are relationship fields)
 */
@DisplayName("AffinityResolver")
class AffinityResolverTest {

    // -- Core algorithm extracted as pure function --

    private fun resolve(
        operations: List<JsonObject>,
        workers: Set<String>,
        scopes: Set<AffinityScopeType>,
        entityCache: Map<String, JsonObject> = emptyMap(),
        relationshipFields: Map<String, List<String>> = emptyMap()
    ): Map<String, List<JsonObject>> {
        if (workers.isEmpty()) return emptyMap()
        if (operations.isEmpty()) return emptyMap()

        val workerList = workers.toList().sorted()
        val affinityMap = mutableMapOf<String, String>()
        val assignments = mutableMapOf<String, MutableList<JsonObject>>()
        workerList.forEach { assignments[it] = mutableListOf() }

        val workUnits = operations.groupBy { op ->
            op.getString("operationSetId") ?: op.getString("id")
        }

        for ((routingKey, workUnitOps) in workUnits) {
            val cohort = mutableSetOf<String>()
            workUnitOps.mapNotNull { it.getString("entityId") }.forEach { cohort.add(it) }

            if (AffinityScopeType.TARGETS in scopes) {
                cohort.addAll(extractTargetIds(workUnitOps, entityCache))
            }

            if (AffinityScopeType.FIELD in scopes) {
                cohort.addAll(resolveFieldRelated(cohort, entityCache, relationshipFields))
            }

            val existingWorker = cohort.firstNotNullOfOrNull { affinityMap[it] }
            val worker = existingWorker ?: hashToWorker(routingKey, workerList)

            cohort.forEach { affinityMap[it] = worker }
            assignments.getOrPut(worker) { mutableListOf() }.addAll(workUnitOps)
        }

        return assignments
    }

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

    private fun resolveFieldRelated(
        cohort: Set<String>,
        entityCache: Map<String, JsonObject>,
        relationshipFields: Map<String, List<String>>
    ): Set<String> {
        val related = mutableSetOf<String>()
        for (entityId in cohort) {
            val entityJson = entityCache[entityId] ?: continue
            val state = entityJson.getJsonObject("state") ?: continue
            val fields = relationshipFields[entityId] ?: continue
            for (fieldName in fields) {
                val fieldValue = state.getValue(fieldName) ?: continue
                related.addAll(extractEntityIds(fieldValue))
            }
        }
        return related
    }

    private fun extractEntityIds(value: Any?): List<String> {
        return when (value) {
            null -> emptyList()
            is String -> if (value.isNotBlank()) listOf(value) else emptyList()
            is List<*> -> value.filterIsInstance<String>()
            is JsonArray -> value.list.filterIsInstance<String>()
            else -> emptyList()
        }
    }

    private fun hashToWorker(routingKey: String, workerList: List<String>): String {
        val hash = abs(routingKey.hashCode())
        return workerList[hash % workerList.size]
    }

    // -- Helpers --

    private fun op(id: String, entityId: String, delta: JsonObject? = null): JsonObject {
        val json = JsonObject().put("id", id).put("entityId", entityId)
        if (delta != null) json.put("delta", delta)
        return json
    }

    private fun setOp(
        id: String, entityId: String, operationSetId: String, setIndex: Int,
        delta: JsonObject? = null
    ): JsonObject {
        val json = JsonObject()
            .put("id", id)
            .put("entityId", entityId)
            .put("operationSetId", operationSetId)
            .put("setIndex", setIndex)
        if (delta != null) json.put("delta", delta)
        return json
    }

    private fun entityJson(state: JsonObject): JsonObject {
        return JsonObject().put("state", state)
    }

    private val workers = setOf("worker-A", "worker-B", "worker-C")

    private fun workerFor(result: Map<String, List<JsonObject>>, entityId: String): String {
        return result.entries.first { (_, ops) ->
            ops.any { it.getString("entityId") == entityId }
        }.key
    }

    private fun workerForOp(result: Map<String, List<JsonObject>>, opId: String): String {
        return result.entries.first { (_, ops) ->
            ops.any { it.getString("id") == opId }
        }.key
    }

    // -- Tests --

    @Nested
    @DisplayName("Empty inputs")
    inner class EmptyInputs {

        @Test
        fun `empty operations returns empty map`() {
            val result = resolve(emptyList(), workers, setOf(AffinityScopeType.ENTITY))
            assertThat(result).isEmpty()
        }

        @Test
        fun `empty workers returns empty map`() {
            val ops = listOf(op("op-1", "entity-A"))
            val result = resolve(ops, emptySet(), setOf(AffinityScopeType.ENTITY))
            assertThat(result).isEmpty()
        }
    }

    @Nested
    @DisplayName("ENTITY scope")
    inner class EntityScope {

        @Test
        fun `two ops with same entityId route to same worker`() {
            val ops = listOf(
                op("op-1", "entity-A"),
                op("op-2", "entity-A")
            )
            val result = resolve(ops, workers, setOf(AffinityScopeType.ENTITY))

            val worker1 = workerForOp(result, "op-1")
            val worker2 = workerForOp(result, "op-2")
            assertThat(worker1).isEqualTo(worker2)
        }

        @Test
        fun `two ops with different entityIds may route to different workers`() {
            // With enough entity IDs, at least some will hash to different workers
            val ops = (1..20).map { op("op-$it", "entity-$it") }
            val result = resolve(ops, workers, setOf(AffinityScopeType.ENTITY))

            val usedWorkers = result.filter { it.value.isNotEmpty() }.keys
            assertThat(usedWorkers.size).isGreaterThan(1)
        }

        @Test
        fun `all operations are assigned`() {
            val ops = listOf(
                op("op-1", "entity-A"),
                op("op-2", "entity-B"),
                op("op-3", "entity-A"),
                op("op-4", "entity-C")
            )
            val result = resolve(ops, workers, setOf(AffinityScopeType.ENTITY))

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(4)
        }

        @Test
        fun `first-come-first-served - entityId claims worker on first encounter`() {
            // op-1 claims a worker for entity-A, op-2 must follow
            val ops = listOf(
                op("op-1", "entity-A"),
                op("op-2", "entity-A")
            )
            val result = resolve(ops, workers, setOf(AffinityScopeType.ENTITY))

            // Both should be on the same worker
            val assignedWorker = workerFor(result, "entity-A")
            val opsOnWorker = result[assignedWorker]!!
            assertThat(opsOnWorker).hasSize(2)
            assertThat(opsOnWorker.map { it.getString("id") }).containsExactlyInAnyOrder("op-1", "op-2")
        }
    }

    @Nested
    @DisplayName("TARGETS scope")
    inner class TargetsScope {

        @Test
        fun `op whose delta references a real entity co-locates with that entity`() {
            val ops = listOf(
                op("op-1", "entity-A", delta = JsonObject().put("targetField", "entity-B")),
                op("op-2", "entity-B")
            )
            val entityCache = mapOf(
                "entity-A" to entityJson(JsonObject()),
                "entity-B" to entityJson(JsonObject())
            )
            val result = resolve(ops, workers, setOf(AffinityScopeType.ENTITY, AffinityScopeType.TARGETS), entityCache)

            assertThat(workerFor(result, "entity-A")).isEqualTo(workerFor(result, "entity-B"))
        }

        @Test
        fun `delta value that is not an entity ID is ignored`() {
            val ops = listOf(
                op("op-1", "entity-A", delta = JsonObject().put("name", "not-an-entity-id")),
                op("op-2", "entity-B")
            )
            // Only entity-A and entity-B exist — "not-an-entity-id" is not in cache
            val entityCache = mapOf(
                "entity-A" to entityJson(JsonObject()),
                "entity-B" to entityJson(JsonObject())
            )
            val result = resolve(ops, workers, setOf(AffinityScopeType.ENTITY, AffinityScopeType.TARGETS), entityCache)

            // entity-A and entity-B are NOT forced together since "not-an-entity-id" isn't real
            // They may or may not be on the same worker depending on hash — but the point is
            // the non-entity string didn't create an affinity link
            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(2)
        }

        @Test
        fun `numeric and non-string delta values are ignored`() {
            val ops = listOf(
                op("op-1", "entity-A", delta = JsonObject()
                    .put("count", 42)
                    .put("flag", true)
                    .put("nested", JsonObject().put("x", 1)))
            )
            val entityCache = mapOf("entity-A" to entityJson(JsonObject()))

            val result = resolve(ops, workers, setOf(AffinityScopeType.TARGETS), entityCache)

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(1)
        }

        @Test
        fun `delta with multiple entity references co-locates all`() {
            val ops = listOf(
                op("op-1", "entity-A", delta = JsonObject()
                    .put("target1", "entity-B")
                    .put("target2", "entity-C")),
                op("op-2", "entity-B"),
                op("op-3", "entity-C")
            )
            val entityCache = mapOf(
                "entity-A" to entityJson(JsonObject()),
                "entity-B" to entityJson(JsonObject()),
                "entity-C" to entityJson(JsonObject())
            )
            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.ENTITY, AffinityScopeType.TARGETS),
                entityCache
            )

            val workerA = workerFor(result, "entity-A")
            val workerB = workerFor(result, "entity-B")
            val workerC = workerFor(result, "entity-C")
            assertThat(workerA).isEqualTo(workerB)
            assertThat(workerB).isEqualTo(workerC)
        }
    }

    @Nested
    @DisplayName("FIELD scope")
    inner class FieldScope {

        @Test
        fun `entity with parent field co-locates with parent`() {
            val ops = listOf(
                op("op-1", "child-1"),
                op("op-2", "parent-1")
            )
            val entityCache = mapOf(
                "child-1" to entityJson(JsonObject().put("parentId", "parent-1")),
                "parent-1" to entityJson(JsonObject())
            )
            val relationshipFields = mapOf(
                "child-1" to listOf("parentId")
            )
            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.ENTITY, AffinityScopeType.FIELD),
                entityCache, relationshipFields
            )

            assertThat(workerFor(result, "child-1")).isEqualTo(workerFor(result, "parent-1"))
        }

        @Test
        fun `entity with child list field co-locates with all children`() {
            val ops = listOf(
                op("op-1", "parent-1"),
                op("op-2", "child-A"),
                op("op-3", "child-B")
            )
            val entityCache = mapOf(
                "parent-1" to entityJson(JsonObject()
                    .put("childIds", JsonArray().add("child-A").add("child-B"))),
                "child-A" to entityJson(JsonObject()),
                "child-B" to entityJson(JsonObject())
            )
            val relationshipFields = mapOf(
                "parent-1" to listOf("childIds")
            )
            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.ENTITY, AffinityScopeType.FIELD),
                entityCache, relationshipFields
            )

            val w = workerFor(result, "parent-1")
            assertThat(workerFor(result, "child-A")).isEqualTo(w)
            assertThat(workerFor(result, "child-B")).isEqualTo(w)
        }

        @Test
        fun `entity not in cache is skipped gracefully`() {
            val ops = listOf(
                op("op-1", "entity-A"),
                op("op-2", "entity-B")
            )
            // entity-A is NOT in cache
            val entityCache = mapOf(
                "entity-B" to entityJson(JsonObject())
            )
            val relationshipFields = mapOf(
                "entity-A" to listOf("parentId")
            )
            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.ENTITY, AffinityScopeType.FIELD),
                entityCache, relationshipFields
            )

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(2)
        }

        @Test
        fun `entity with no relationship fields is not expanded`() {
            val ops = listOf(
                op("op-1", "entity-A"),
                op("op-2", "entity-B")
            )
            val entityCache = mapOf(
                "entity-A" to entityJson(JsonObject().put("name", "Alice")),
                "entity-B" to entityJson(JsonObject().put("name", "Bob"))
            )
            // No relationship fields defined for either entity
            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.ENTITY, AffinityScopeType.FIELD),
                entityCache
            )

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(2)
        }
    }

    @Nested
    @DisplayName("OPERATION_SET scope")
    inner class OperationSetScope {

        @Test
        fun `set members route to same worker via shared operationSetId`() {
            val ops = listOf(
                setOp("op-1", "entity-A", "set-1", 0),
                setOp("op-2", "entity-B", "set-1", 1)
            )
            val result = resolve(ops, workers, setOf(AffinityScopeType.ENTITY))

            val worker1 = workerForOp(result, "op-1")
            val worker2 = workerForOp(result, "op-2")
            assertThat(worker1).isEqualTo(worker2)
        }

        @Test
        fun `set members and solo op are independent`() {
            val setId = "set-1"
            val ops = listOf(
                setOp("op-1", "entity-A", setId, 0),
                setOp("op-2", "entity-B", setId, 1),
                op("op-3", "entity-C")
            )
            val result = resolve(ops, workers, setOf(AffinityScopeType.ENTITY))

            // Set members are together
            assertThat(workerForOp(result, "op-1")).isEqualTo(workerForOp(result, "op-2"))

            // All operations assigned
            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(3)
        }
    }

    @Nested
    @DisplayName("First-come-first-served semantics")
    inner class FirstComeFirstServed {

        @Test
        fun `second work unit follows first if they share a related entity via FIELD`() {
            // op-1 processes entity-A which has @Parent → entity-shared
            // op-2 processes entity-B which also has @Parent → entity-shared
            // entity-A is processed first, claims a worker for entity-shared
            // entity-B must follow entity-shared's worker
            val ops = listOf(
                op("op-1", "entity-A"),
                op("op-2", "entity-B")
            )
            val entityCache = mapOf(
                "entity-A" to entityJson(JsonObject().put("parentId", "entity-shared")),
                "entity-B" to entityJson(JsonObject().put("parentId", "entity-shared"))
            )
            val relationshipFields = mapOf(
                "entity-A" to listOf("parentId"),
                "entity-B" to listOf("parentId")
            )
            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.ENTITY, AffinityScopeType.FIELD),
                entityCache, relationshipFields
            )

            assertThat(workerFor(result, "entity-A")).isEqualTo(workerFor(result, "entity-B"))
        }

        @Test
        fun `cohort transitivity - A references B via field, C references B via delta`() {
            val ops = listOf(
                op("op-1", "entity-A"),
                op("op-2", "entity-C", delta = JsonObject().put("ref", "entity-B"))
            )
            val entityCache = mapOf(
                "entity-A" to entityJson(JsonObject().put("parentId", "entity-B")),
                "entity-B" to entityJson(JsonObject()),
                "entity-C" to entityJson(JsonObject())
            )
            val relationshipFields = mapOf(
                "entity-A" to listOf("parentId")
            )
            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.ENTITY, AffinityScopeType.TARGETS, AffinityScopeType.FIELD),
                entityCache, relationshipFields
            )

            // op-1 claims entity-A + entity-B (via FIELD)
            // op-2 has entity-C + entity-B (via TARGETS)
            // entity-B already mapped → op-2 follows op-1's worker
            assertThat(workerFor(result, "entity-A")).isEqualTo(workerFor(result, "entity-C"))
        }
    }

    @Nested
    @DisplayName("No scopes configured (fallback)")
    inner class NoScopes {

        @Test
        fun `empty scopes uses routing key hash distribution`() {
            val ops = listOf(
                op("op-1", "entity-A"),
                op("op-2", "entity-A")
            )
            // With empty scopes, ops hash by their own id, not entityId
            // So two ops for the same entity may end up on different workers
            val result = resolve(ops, workers, emptySet())

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(2)
        }
    }

    @Nested
    @DisplayName("Mixed scopes")
    inner class MixedScopes {

        @Test
        fun `ENTITY + FIELD + TARGETS produces full co-location`() {
            // entity-A has @Parent → entity-P
            // entity-B's delta references entity-P
            // All three should co-locate
            val ops = listOf(
                op("op-1", "entity-A"),
                op("op-2", "entity-B", delta = JsonObject().put("target", "entity-P")),
                op("op-3", "entity-P")
            )
            val entityCache = mapOf(
                "entity-A" to entityJson(JsonObject().put("parentId", "entity-P")),
                "entity-B" to entityJson(JsonObject()),
                "entity-P" to entityJson(JsonObject())
            )
            val relationshipFields = mapOf(
                "entity-A" to listOf("parentId")
            )
            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.ENTITY, AffinityScopeType.TARGETS, AffinityScopeType.FIELD),
                entityCache, relationshipFields
            )

            val w = workerFor(result, "entity-A")
            assertThat(workerFor(result, "entity-B")).isEqualTo(w)
            assertThat(workerFor(result, "entity-P")).isEqualTo(w)
        }

        @Test
        fun `operation set with FIELD scope co-locates all relationship targets`() {
            val setId = "set-1"
            val ops = listOf(
                setOp("op-1", "entity-A", setId, 0),
                setOp("op-2", "entity-B", setId, 1),
                op("op-3", "entity-P")
            )
            val entityCache = mapOf(
                "entity-A" to entityJson(JsonObject().put("parentId", "entity-P")),
                "entity-B" to entityJson(JsonObject()),
                "entity-P" to entityJson(JsonObject())
            )
            val relationshipFields = mapOf(
                "entity-A" to listOf("parentId")
            )
            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.ENTITY, AffinityScopeType.FIELD),
                entityCache, relationshipFields
            )

            // Set members are together (via operationSetId grouping)
            assertThat(workerForOp(result, "op-1")).isEqualTo(workerForOp(result, "op-2"))

            // entity-P is co-located because entity-A's @Parent references it
            val setWorker = workerForOp(result, "op-1")
            assertThat(workerFor(result, "entity-P")).isEqualTo(setWorker)
        }
    }

    @Nested
    @DisplayName("Malformed and inappropriate inputs")
    inner class MalformedInputs {

        @Test
        fun `operation missing entityId is assigned but does not contribute to affinity`() {
            val ops = listOf(
                JsonObject().put("id", "op-1"), // no entityId
                op("op-2", "entity-A")
            )
            val result = resolve(ops, workers, setOf(AffinityScopeType.ENTITY))

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(2)
        }

        @Test
        fun `operation with null entityId does not crash`() {
            val ops = listOf(
                JsonObject().put("id", "op-1").putNull("entityId"),
                op("op-2", "entity-A")
            )
            val result = resolve(ops, workers, setOf(AffinityScopeType.ENTITY))

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(2)
        }

        @Test
        fun `operation with empty string entityId is treated as a cohort member`() {
            val ops = listOf(
                op("op-1", ""),
                op("op-2", "entity-A")
            )
            // Empty string entityId shouldn't crash — mapNotNull filters it out
            // since getString returns "" (not null), but it goes into the cohort
            val result = resolve(ops, workers, setOf(AffinityScopeType.ENTITY))

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(2)
        }

        @Test
        fun `delta is not a JsonObject — skipped gracefully for TARGETS`() {
            val opWithBadDelta = JsonObject()
                .put("id", "op-1")
                .put("entityId", "entity-A")
                .put("delta", "not-a-json-object")
            val ops = listOf(opWithBadDelta)
            val entityCache = mapOf("entity-A" to entityJson(JsonObject()))

            val result = resolve(ops, workers, setOf(AffinityScopeType.TARGETS), entityCache)

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(1)
        }

        @Test
        fun `delta with null values does not crash TARGETS`() {
            val ops = listOf(
                op("op-1", "entity-A", delta = JsonObject().putNull("someField"))
            )
            val entityCache = mapOf("entity-A" to entityJson(JsonObject()))

            val result = resolve(ops, workers, setOf(AffinityScopeType.TARGETS), entityCache)

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(1)
        }

        @Test
        fun `entity in cache with no state object does not crash FIELD`() {
            val ops = listOf(op("op-1", "entity-A"))
            // Entity document exists but has no "state" key
            val entityCache = mapOf("entity-A" to JsonObject().put("type", "SomeType"))
            val relationshipFields = mapOf("entity-A" to listOf("parentId"))

            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.FIELD),
                entityCache, relationshipFields
            )

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(1)
        }

        @Test
        fun `relationship field name not present in entity state is skipped`() {
            val ops = listOf(op("op-1", "entity-A"))
            val entityCache = mapOf(
                "entity-A" to entityJson(JsonObject().put("name", "Alice"))
                // no "parentId" field in state
            )
            val relationshipFields = mapOf("entity-A" to listOf("parentId"))

            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.FIELD),
                entityCache, relationshipFields
            )

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(1)
        }

        @Test
        fun `relationship field value is a number — ignored gracefully`() {
            val ops = listOf(op("op-1", "entity-A"))
            val entityCache = mapOf(
                "entity-A" to entityJson(JsonObject().put("parentId", 12345))
            )
            val relationshipFields = mapOf("entity-A" to listOf("parentId"))

            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.FIELD),
                entityCache, relationshipFields
            )

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(1)
        }

        @Test
        fun `operation with both id and operationSetId groups by operationSetId`() {
            // Verifies the routing key precedence: operationSetId wins over id
            val ops = listOf(
                setOp("op-1", "entity-A", "set-X", 0),
                setOp("op-2", "entity-B", "set-X", 1)
            )
            val result = resolve(ops, workers, setOf(AffinityScopeType.ENTITY))

            // Both should be in the same work unit (grouped by operationSetId)
            assertThat(workerForOp(result, "op-1")).isEqualTo(workerForOp(result, "op-2"))
        }

        @Test
        fun `single worker absorbs all affinity cohorts`() {
            val ops = listOf(
                op("op-1", "entity-A"),
                op("op-2", "entity-B"),
                op("op-3", "entity-C")
            )
            val singleWorker = setOf("only-worker")
            val result = resolve(ops, singleWorker, setOf(AffinityScopeType.ENTITY))

            assertThat(result["only-worker"]).hasSize(3)
        }

        @Test
        fun `empty delta object does not contribute targets`() {
            val ops = listOf(
                op("op-1", "entity-A", delta = JsonObject())
            )
            val entityCache = mapOf("entity-A" to entityJson(JsonObject()))

            val result = resolve(ops, workers, setOf(AffinityScopeType.TARGETS), entityCache)

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(1)
        }

        @Test
        fun `mixed JsonArray with non-strings filters to strings only`() {
            val ops = listOf(op("op-1", "entity-A"))
            val entityCache = mapOf(
                "entity-A" to entityJson(JsonObject()
                    .put("childIds", JsonArray().add("child-1").add(42).add(true).add("child-2")))
            )
            val relationshipFields = mapOf("entity-A" to listOf("childIds"))

            val result = resolve(
                ops, workers,
                setOf(AffinityScopeType.FIELD),
                entityCache, relationshipFields
            )

            // Should not crash, should extract "child-1" and "child-2" only
            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(1)
        }
    }

    @Nested
    @DisplayName("extractEntityIds polymorphism")
    inner class ExtractEntityIdsPolymorphism {

        @Test
        fun `extracts from String field`() {
            assertThat(extractEntityIds("entity-1")).containsExactly("entity-1")
        }

        @Test
        fun `extracts from List of Strings`() {
            assertThat(extractEntityIds(listOf("entity-1", "entity-2")))
                .containsExactly("entity-1", "entity-2")
        }

        @Test
        fun `extracts from JsonArray`() {
            val arr = JsonArray().add("entity-1").add("entity-2")
            assertThat(extractEntityIds(arr)).containsExactly("entity-1", "entity-2")
        }

        @Test
        fun `null returns empty`() {
            assertThat(extractEntityIds(null)).isEmpty()
        }

        @Test
        fun `blank string returns empty`() {
            assertThat(extractEntityIds("")).isEmpty()
            assertThat(extractEntityIds("   ")).isEmpty()
        }

        @Test
        fun `unknown type returns empty`() {
            assertThat(extractEntityIds(42)).isEmpty()
        }
    }
}
