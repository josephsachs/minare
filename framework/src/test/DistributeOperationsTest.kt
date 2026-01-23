package com.minare.core.frames.coordinator.services

import io.vertx.core.json.JsonObject
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import kotlin.math.abs

/**
 * Tests for the distributeOperations algorithm.
 *
 * This tests the PURE FUNCTION logic extracted from FrameManifestBuilder.
 * The actual FrameManifestBuilder class has Hazelcast dependencies,
 * but distributeOperations() is a pure function we can test directly.
 */
@DisplayName("FrameManifestBuilder.distributeOperations")
class DistributeOperationsTest {

    private fun distributeOperations(
        operations: List<JsonObject>,
        workers: Set<String>
    ): Map<String, List<JsonObject>> {
        if (workers.isEmpty()) return emptyMap()

        val workerList = workers.toList().sorted()

        return operations.groupBy { op ->
            val operationId = op.getString("id")
            val hash = abs(operationId.hashCode())
            workerList[hash % workerList.size]
        }
    }

    private fun createOperations(count: Int): List<JsonObject> {
        return (1..count).map { JsonObject().put("id", "op-$it") }
    }

    private fun createOperationsWithIds(ids: List<String>): List<JsonObject> {
        return ids.map { JsonObject().put("id", it) }
    }

    @Nested
    @DisplayName("Basic Distribution")
    inner class BasicDistribution {

        @Test
        fun `10 operations, 3 workers distributes all operations`() {
            val operations = createOperations(10)
            val workers = setOf("worker-A", "worker-B", "worker-C")

            val result = distributeOperations(operations, workers)

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(10)
        }

        @Test
        fun `10 operations, 3 workers in different order produces identical distribution`() {
            val operations = createOperations(10)
            val workers1 = setOf("worker-A", "worker-B", "worker-C")
            val workers2 = setOf("worker-C", "worker-A", "worker-B")
            val workers3 = setOf("worker-B", "worker-C", "worker-A")

            val result1 = distributeOperations(operations, workers1)
            val result2 = distributeOperations(operations, workers2)
            val result3 = distributeOperations(operations, workers3)

            assertThat(result1).isEqualTo(result2)
            assertThat(result2).isEqualTo(result3)
        }

        @Test
        fun `100 operations, 5 workers assigns all operations`() {
            val operations = createOperations(100)
            val workers = setOf("w1", "w2", "w3", "w4", "w5")

            val result = distributeOperations(operations, workers)

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(100)
        }

        @Test
        fun `3 operations, 5 workers leaves some workers with empty lists`() {
            val operations = createOperations(3)
            val workers = setOf("w1", "w2", "w3", "w4", "w5")

            val result = distributeOperations(operations, workers)

            val workersWithOps = result.filter { it.value.isNotEmpty() }.size
            assertThat(workersWithOps).isLessThanOrEqualTo(3)
        }

        @Test
        fun `single worker gets all operations`() {
            val operations = createOperations(10)
            val workers = setOf("only-worker")

            val result = distributeOperations(operations, workers)

            assertThat(result).hasSize(1)
            assertThat(result["only-worker"]).hasSize(10)
        }

        @Test
        fun `empty operations list returns empty map`() {
            val operations = emptyList<JsonObject>()
            val workers = setOf("worker-A", "worker-B")

            val result = distributeOperations(operations, workers)

            assertThat(result).isEmpty()
        }

        @Test
        fun `empty workers set returns empty map`() {
            val operations = createOperations(10)
            val workers = emptySet<String>()

            val result = distributeOperations(operations, workers)

            assertThat(result).isEmpty()
        }
    }

    @Nested
    @DisplayName("Determinism & Consistency")
    inner class DeterminismAndConsistency {

        @Test
        fun `same operation ID always hashes to same worker given same worker set`() {
            val workers = setOf("worker-A", "worker-B", "worker-C")
            val operationId = "test-operation-123"
            val operation = JsonObject().put("id", operationId)

            val results = (1..100).map {
                distributeOperations(listOf(operation), workers)
            }

            val assignedWorkers = results.map { result ->
                result.entries.first { it.value.isNotEmpty() }.key
            }.toSet()

            assertThat(assignedWorkers).hasSize(1)
        }

        @Test
        fun `operation IDs that collide on same worker are all assigned`() {
            val workers = setOf("worker-A", "worker-B")
            val workerList = workers.toList().sorted()

            val targetWorker = workerList[0]
            val collidingOps = (1..1000).map { "op-$it" }
                .filter { id ->
                    val hash = abs(id.hashCode())
                    workerList[hash % workerList.size] == targetWorker
                }
                .take(5)

            assertThat(collidingOps).isNotEmpty()

            val operations = createOperationsWithIds(collidingOps)
            val result = distributeOperations(operations, workers)

            assertThat(result[targetWorker]).hasSize(collidingOps.size)
        }

        @Test
        fun `re-running with identical inputs produces identical output`() {
            val operations = createOperations(50)
            val workers = setOf("alpha", "beta", "gamma", "delta")

            val result1 = distributeOperations(operations, workers)
            val result2 = distributeOperations(operations, workers)

            assertThat(result1).isEqualTo(result2)
        }

        @Test
        fun `1000 iterations produce identical results`() {
            val operations = createOperations(20)
            val workers = setOf("w1", "w2", "w3")

            val firstResult = distributeOperations(operations, workers)

            repeat(1000) {
                val result = distributeOperations(operations, workers)
                assertThat(result).isEqualTo(firstResult)
            }
        }
    }

    @Nested
    @DisplayName("Edge Cases")
    inner class EdgeCases {

        @Test
        fun `operation with null ID is handled`() {
            val operation = JsonObject()
            val workers = setOf("worker-A", "worker-B")

            val result = runCatching {
                distributeOperations(listOf(operation), workers)
            }

            // Document behavior: getString returns null, hashCode of null throws NPE
            // This test documents that null IDs cause failure
            assertThat(result.isFailure).isTrue()
        }

        @Test
        fun `operation with empty string ID is handled gracefully`() {
            val operation = JsonObject().put("id", "")
            val workers = setOf("worker-A", "worker-B")

            val result = distributeOperations(listOf(operation), workers)

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(1)
        }

        @Test
        fun `workers with special characters in IDs`() {
            val operations = createOperations(10)
            val workers = setOf(
                "worker:1",
                "worker/2",
                "worker-3",
                "worker_4",
                "worker.5"
            )

            val result = distributeOperations(operations, workers)

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(10)
        }

        @Test
        fun `very large operation sets distribute correctly`() {
            val operations = createOperations(10_000)
            val workers = setOf("w1", "w2", "w3", "w4", "w5")

            val result = distributeOperations(operations, workers)

            val totalAssigned = result.values.sumOf { it.size }
            assertThat(totalAssigned).isEqualTo(10_000)

            result.forEach { (_, ops) ->
                assertThat(ops.size).isGreaterThan(0)
            }
        }

        @Test
        fun `distribution is reasonably even for large operation sets`() {
            val operations = createOperations(10_000)
            val workers = setOf("w1", "w2", "w3", "w4", "w5")

            val result = distributeOperations(operations, workers)

            val counts = result.values.map { it.size }
            val min = counts.minOrNull() ?: 0
            val max = counts.maxOrNull() ?: 0

            // With 10k operations and 5 workers, expect ~2000 each
            // Allow for hash distribution variance but flag extreme skew
            assertThat(min).isGreaterThan(1000)
            assertThat(max).isLessThan(3000)
        }
    }

    @Nested
    @DisplayName("Worker Sorting Requirement")
    inner class WorkerSortingRequirement {

        /**
         * Sorting workers ensures deterministic assignment regardless of
         * Set iteration order or how workers are provided.
         */
        @Test
        fun `sorted worker list is deterministic regardless of input order`() {
            val operationId = "test-op-456"

            val orders = listOf(
                listOf("A", "B", "C"),
                listOf("C", "B", "A"),
                listOf("B", "A", "C"),
                listOf("A", "C", "B")
            )

            val assignments = orders.map { workers ->
                val sorted = workers.sorted()
                val hash = abs(operationId.hashCode())
                sorted[hash % sorted.size]
            }

            assertThat(assignments.toSet()).hasSize(1)
        }

        /**
         * Demonstrates that unsorted lists can produce different assignments.
         * This is why all code paths that assign operations must sort workers first.
         */
        @Test
        fun `unsorted worker lists can produce different assignments for same operation`() {
            val operationId = "test-op-123"

            val order1 = listOf("worker-B", "worker-A", "worker-C")
            val order2 = listOf("worker-A", "worker-B", "worker-C")

            val hash = abs(operationId.hashCode())
            val assigned1 = order1[hash % order1.size]
            val assigned2 = order2[hash % order2.size]

            // These may or may not be equal depending on hash value
            // The point is: without sorting, we can't guarantee consistency
            // This test just documents the behavior exists

            // Find operation IDs where different orderings produce different results
            val differingIds = (1..1000).map { "op-$it" }.filter { id ->
                val h = abs(id.hashCode())
                order1[h % order1.size] != order2[h % order2.size]
            }

            assertThat(differingIds)
                .withFailMessage("Expected to find IDs that hash differently with different orderings")
                .isNotEmpty()
        }
    }
}