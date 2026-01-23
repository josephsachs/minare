package com.minare.core.frames.coordinator.services

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test

/**
 * Tests for Worker Topology Change Detection.
 *
 * CRITICAL CONSTRAINT: Worker topology MUST remain fixed per session.
 * Changing workers mid-session corrupts determinism because the hash ring changes.
 *
 * These tests specify the behavior for a feature that needs to be implemented:
 * - Track lastKnownWorkers and sessionId in FrameManifestBuilder
 * - Detect topology changes and publish violation events
 * - Throw WorkerTopologyChangedException
 */
@DisplayName("Worker Topology Change Detection")
class WorkerTopologyChangeDetectionTest {

    /**
     * Simulates the topology-aware distributeOperations that needs to be implemented.
     * This captures the events that would be published.
     */
    class TopologyAwareDistributor {
        var lastKnownWorkers: Set<String>? = null
        var sessionId: String? = null
        val publishedEvents = mutableListOf<JsonObject>()

        fun distributeOperations(
            operations: List<JsonObject>,
            workers: Set<String>,
            logicalFrame: Long,
            currentSessionId: String
        ): Map<String, List<JsonObject>> {

            // First call: store worker set and session
            if (lastKnownWorkers == null) {
                lastKnownWorkers = workers
                sessionId = currentSessionId
            } else {
                // Subsequent calls: verify topology unchanged
                if (workers != lastKnownWorkers) {
                    val event = JsonObject()
                        .put("expectedWorkers", JsonArray(lastKnownWorkers!!.toList()))
                        .put("actualWorkers", JsonArray(workers.toList()))
                        .put("sessionId", sessionId)
                        .put("frameNumber", logicalFrame)
                        .put("detectedAt", System.currentTimeMillis())

                    publishedEvents.add(event)

                    throw WorkerTopologyChangedException(
                        "Worker topology changed during session $sessionId at frame $logicalFrame. " +
                                "Expected: $lastKnownWorkers, Got: $workers"
                    )
                }
            }

            // Actual distribution logic (simplified)
            if (workers.isEmpty()) return emptyMap()
            val workerList = workers.toList().sorted()
            return operations.groupBy { op ->
                val operationId = op.getString("id")
                val hash = kotlin.math.abs(operationId.hashCode())
                workerList[hash % workerList.size]
            }
        }

        fun reset() {
            lastKnownWorkers = null
            sessionId = null
            publishedEvents.clear()
        }
    }

    class WorkerTopologyChangedException(message: String) : RuntimeException(message)

    private lateinit var distributor: TopologyAwareDistributor

    private fun createOperations(count: Int): List<JsonObject> {
        return (1..count).map { JsonObject().put("id", "op-$it") }
    }

    @BeforeEach
    fun setUp() {
        distributor = TopologyAwareDistributor()
    }

    @Nested
    @DisplayName("First Distribution")
    inner class FirstDistribution {

        @Test
        fun `first distribution with workers stores worker set and succeeds`() {
            val operations = createOperations(5)
            val workers = setOf("A", "B", "C")

            val result = distributor.distributeOperations(operations, workers, 0, "session-1")

            assertThat(result.values.sumOf { it.size }).isEqualTo(5)
            assertThat(distributor.lastKnownWorkers).isEqualTo(workers)
            assertThat(distributor.sessionId).isEqualTo("session-1")
        }

        @Test
        fun `first distribution publishes no events`() {
            val operations = createOperations(5)
            val workers = setOf("A", "B", "C")

            distributor.distributeOperations(operations, workers, 0, "session-1")

            assertThat(distributor.publishedEvents).isEmpty()
        }
    }

    @Nested
    @DisplayName("Subsequent Distributions - Same Topology")
    inner class SameTopology {

        @Test
        fun `second distribution with same workers succeeds`() {
            val operations = createOperations(5)
            val workers = setOf("A", "B", "C")

            distributor.distributeOperations(operations, workers, 0, "session-1")
            val result = distributor.distributeOperations(operations, workers, 1, "session-1")

            assertThat(result.values.sumOf { it.size }).isEqualTo(5)
            assertThat(distributor.publishedEvents).isEmpty()
        }

        @Test
        fun `many distributions with same workers all succeed`() {
            val operations = createOperations(5)
            val workers = setOf("A", "B", "C")

            repeat(100) { frame ->
                val result = distributor.distributeOperations(
                    operations, workers, frame.toLong(), "session-1"
                )
                assertThat(result.values.sumOf { it.size }).isEqualTo(5)
            }

            assertThat(distributor.publishedEvents).isEmpty()
        }
    }

    @Nested
    @DisplayName("Topology Violations")
    inner class TopologyViolations {

        @Test
        fun `changed worker throws WorkerTopologyChangedException`() {
            val operations = createOperations(5)
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("A", "B", "D")

            distributor.distributeOperations(operations, workers1, 0, "session-1")

            assertThatThrownBy {
                distributor.distributeOperations(operations, workers2, 1, "session-1")
            }.isInstanceOf(WorkerTopologyChangedException::class.java)
        }

        @Test
        fun `added worker throws WorkerTopologyChangedException`() {
            val operations = createOperations(5)
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("A", "B", "C", "D")

            distributor.distributeOperations(operations, workers1, 0, "session-1")

            assertThatThrownBy {
                distributor.distributeOperations(operations, workers2, 1, "session-1")
            }.isInstanceOf(WorkerTopologyChangedException::class.java)
        }

        @Test
        fun `removed worker throws WorkerTopologyChangedException`() {
            val operations = createOperations(5)
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("A", "B")

            distributor.distributeOperations(operations, workers1, 0, "session-1")

            assertThatThrownBy {
                distributor.distributeOperations(operations, workers2, 1, "session-1")
            }.isInstanceOf(WorkerTopologyChangedException::class.java)
        }
    }

    @Nested
    @DisplayName("Exception Message Content")
    inner class ExceptionMessageContent {

        @Test
        fun `exception message includes expected workers`() {
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("A", "B", "D")

            distributor.distributeOperations(createOperations(1), workers1, 0, "session-1")

            assertThatThrownBy {
                distributor.distributeOperations(createOperations(1), workers2, 5, "session-1")
            }.hasMessageContaining("A")
                .hasMessageContaining("B")
                .hasMessageContaining("C")
        }

        @Test
        fun `exception message includes actual workers`() {
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("X", "Y", "Z")

            distributor.distributeOperations(createOperations(1), workers1, 0, "session-1")

            assertThatThrownBy {
                distributor.distributeOperations(createOperations(1), workers2, 5, "session-1")
            }.hasMessageContaining("X")
                .hasMessageContaining("Y")
                .hasMessageContaining("Z")
        }

        @Test
        fun `exception message includes session ID`() {
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("A", "B", "D")

            distributor.distributeOperations(createOperations(1), workers1, 0, "my-session-123")

            assertThatThrownBy {
                distributor.distributeOperations(createOperations(1), workers2, 5, "my-session-123")
            }.hasMessageContaining("my-session-123")
        }

        @Test
        fun `exception message includes frame number`() {
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("A", "B", "D")

            distributor.distributeOperations(createOperations(1), workers1, 0, "session-1")

            assertThatThrownBy {
                distributor.distributeOperations(createOperations(1), workers2, 42, "session-1")
            }.hasMessageContaining("42")
        }
    }

    @Nested
    @DisplayName("Event Payload Content")
    inner class EventPayloadContent {

        @Test
        fun `event is published before exception is thrown`() {
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("A", "B", "D")

            distributor.distributeOperations(createOperations(1), workers1, 0, "session-1")

            runCatching {
                distributor.distributeOperations(createOperations(1), workers2, 5, "session-1")
            }

            assertThat(distributor.publishedEvents).hasSize(1)
        }

        @Test
        fun `event contains expectedWorkers array`() {
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("A", "B", "D")

            distributor.distributeOperations(createOperations(1), workers1, 0, "session-1")
            runCatching {
                distributor.distributeOperations(createOperations(1), workers2, 5, "session-1")
            }

            val event = distributor.publishedEvents.first()
            val expectedWorkers = event.getJsonArray("expectedWorkers").list
            assertThat(expectedWorkers).containsExactlyInAnyOrder("A", "B", "C")
        }

        @Test
        fun `event contains actualWorkers array`() {
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("A", "B", "D")

            distributor.distributeOperations(createOperations(1), workers1, 0, "session-1")
            runCatching {
                distributor.distributeOperations(createOperations(1), workers2, 5, "session-1")
            }

            val event = distributor.publishedEvents.first()
            val actualWorkers = event.getJsonArray("actualWorkers").list
            assertThat(actualWorkers).containsExactlyInAnyOrder("A", "B", "D")
        }

        @Test
        fun `event contains sessionId`() {
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("A", "B", "D")

            distributor.distributeOperations(createOperations(1), workers1, 0, "my-session-456")
            runCatching {
                distributor.distributeOperations(createOperations(1), workers2, 5, "my-session-456")
            }

            val event = distributor.publishedEvents.first()
            assertThat(event.getString("sessionId")).isEqualTo("my-session-456")
        }

        @Test
        fun `event contains frameNumber equal to logicalFrame parameter`() {
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("A", "B", "D")

            distributor.distributeOperations(createOperations(1), workers1, 0, "session-1")
            runCatching {
                distributor.distributeOperations(createOperations(1), workers2, 99, "session-1")
            }

            val event = distributor.publishedEvents.first()
            assertThat(event.getLong("frameNumber")).isEqualTo(99)
        }

        @Test
        fun `event contains detectedAt timestamp`() {
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("A", "B", "D")

            val beforeTime = System.currentTimeMillis()
            distributor.distributeOperations(createOperations(1), workers1, 0, "session-1")
            runCatching {
                distributor.distributeOperations(createOperations(1), workers2, 5, "session-1")
            }
            val afterTime = System.currentTimeMillis()

            val event = distributor.publishedEvents.first()
            val detectedAt = event.getLong("detectedAt")
            assertThat(detectedAt).isBetween(beforeTime, afterTime)
        }
    }

    @Nested
    @DisplayName("Session Reset")
    inner class SessionReset {

        @Test
        fun `after reset, new worker set is accepted`() {
            val workers1 = setOf("A", "B", "C")
            val workers2 = setOf("X", "Y", "Z")

            distributor.distributeOperations(createOperations(1), workers1, 0, "session-1")
            distributor.reset()

            // Should not throw - new session can have different workers
            val result = distributor.distributeOperations(createOperations(5), workers2, 0, "session-2")

            assertThat(result.values.sumOf { it.size }).isEqualTo(5)
            assertThat(distributor.lastKnownWorkers).isEqualTo(workers2)
            assertThat(distributor.sessionId).isEqualTo("session-2")
        }
    }

    @Nested
    @DisplayName("Why This Matters - Determinism Corruption")
    inner class DeterminismCorruptionDocumentation {

        /**
         * This test demonstrates WHY topology changes break determinism.
         * When worker set changes, the same operation ID hashes to a different worker.
         */
        @Test
        fun `same operation ID hashes to different worker when topology changes`() {
            val operationId = "critical-op-123"

            val workers3 = listOf("A", "B", "C").sorted()
            val workers4 = listOf("A", "B", "C", "D").sorted()

            val hash = kotlin.math.abs(operationId.hashCode())

            val workerWith3 = workers3[hash % workers3.size]
            val workerWith4 = workers4[hash % workers4.size]

            // Find an operation ID where they differ to prove the point
            val differingIds = (1..1000).map { "op-$it" }.filter { id ->
                val h = kotlin.math.abs(id.hashCode())
                workers3[h % workers3.size] != workers4[h % workers4.size]
            }

            assertThat(differingIds)
                .withFailMessage(
                    "Adding a worker changes which worker receives operations. " +
                            "This breaks replay determinism - the same operations would be " +
                            "distributed differently, producing different state."
                )
                .isNotEmpty()
        }
    }
}