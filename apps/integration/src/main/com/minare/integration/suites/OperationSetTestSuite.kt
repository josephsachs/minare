package com.minare.integration.suites

import com.google.inject.Injector
import com.hazelcast.core.HazelcastInstance
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.operation.models.Operation
import com.minare.core.operation.models.OperationSet
import com.minare.core.operation.models.OperationType
import com.minare.core.storage.interfaces.StateStore
import com.minare.integration.harness.Assertions.assertEquals
import com.minare.integration.harness.Assertions.assertNotNull
import com.minare.integration.harness.OperationObserver
import com.minare.integration.harness.TestClient
import com.minare.integration.harness.TestRunner
import com.minare.integration.harness.TestStepLog
import com.minare.integration.harness.TestSuite
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.TimeoutCancellationException
import org.slf4j.LoggerFactory

/**
 * Integration tests for OperationSet routing, ordering, and failure policy.
 *
 * Routing and ordering tests verify that operations stamped with the same operationSetId
 * land on the same worker in declaration order. These require OPERATION_SET in
 * frames.group_operations_by.
 *
 * submitSet sends each built operation as command: "operation". The server-side handler
 * for that command must extract operationSetId and setIndex from the payload and propagate
 * them into the Operation before coordinator submission.
 *
 * FailurePolicy, FunctionCall, Assert, and Trigger tests are one step ahead —
 * they will fail until worker-side execution of those step types is implemented.
 */
class OperationSetTestSuite(private val injector: Injector) : TestSuite {
    override val name = "OperationSet Tests"
    private val log = LoggerFactory.getLogger(OperationSetTestSuite::class.java)

    override suspend fun run(runner: TestRunner) {
        val vertx = injector.getInstance(Vertx::class.java)
        val stateStore = injector.getInstance(StateStore::class.java)
        val hazelcast = injector.getInstance(HazelcastInstance::class.java)
        val workerRegistry = injector.getInstance(WorkerRegistry::class.java)
        val hostName = workerRegistry.getAllWorkers().firstNotNullOf { it.key }

        // ── ROUTING ──────────────────────────────────────────────────────────────

        runner.test("SET: all mutations in a set are applied on success") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)

                val (entityId, _) = createEntity(client, observer, log, "SetAllTarget", "#000000")

                val set = OperationSet()
                    .add(Operation().entity(entityId).entityType("Node").action(OperationType.MUTATE)
                        .delta(JsonObject().put("color", "#AA0000")))
                    .add(Operation().entity(entityId).entityType("Node").action(OperationType.MUTATE)
                        .delta(JsonObject().put("label", "Updated")))

                log.step("submitting set", "setId" to set.id, "size" to set.size())
                submitSet(client, connectionId, set)

                awaitEntityUpdate(client, observer, log, "first mutation") { id, _ -> id == entityId }
                awaitEntityUpdate(client, observer, log, "second mutation") { id, _ -> id == entityId }

                val stored = stateStore.findOneJson(entityId)
                assertNotNull(stored) { "Entity should exist in StateStore" }

                assertEquals("#AA0000", stored!!.getJsonObject("state")?.getString("color")) {
                    "First mutation (color) should be applied — got: ${stored.getJsonObject("state")?.getString("color")}"
                }
                assertEquals("Updated", stored.getJsonObject("state")?.getString("label")) {
                    "Second mutation (label) should be applied — got: ${stored.getJsonObject("state")?.getString("label")}"
                }

                log.step("both mutations verified in StateStore",
                    "color" to stored.getJsonObject("state")?.getString("color"),
                    "label" to stored.getJsonObject("state")?.getString("label"))

            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        runner.test("SET: members route to the same worker") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)

                val (entity1Id, _) = createEntity(client, observer, log, "ColocTarget1", "#111111")
                val (entity2Id, _) = createEntity(client, observer, log, "ColocTarget2", "#222222")

                val op1 = Operation().entity(entity1Id).entityType("Node").action(OperationType.MUTATE)
                    .delta(JsonObject().put("color", "#AAAAAA"))
                val op2 = Operation().entity(entity2Id).entityType("Node").action(OperationType.MUTATE)
                    .delta(JsonObject().put("color", "#BBBBBB"))

                val set = OperationSet().add(op1).add(op2)

                log.step("submitting set", "setId" to set.id, "op1" to op1.id, "op2" to op2.id)
                submitSet(client, connectionId, set)

                awaitEntityUpdate(client, observer, log, "op1 broadcast") { id, _ -> id == entity1Id }
                awaitEntityUpdate(client, observer, log, "op2 broadcast") { id, _ -> id == entity2Id }

                val c1 = observer.queryCompletion(op1.id)
                val c2 = observer.queryCompletion(op2.id)

                assertNotNull(c1) { "Completion record should exist for op1 (${op1.id})" }
                assertNotNull(c2) { "Completion record should exist for op2 (${op2.id})" }
                assertEquals(c1!!.workerId, c2!!.workerId) {
                    "Both set members should route to the same worker — op1: ${c1.workerId}, op2: ${c2.workerId}"
                }

                log.step("co-location verified", "worker" to c1.workerId)

            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        runner.test("SET: operations execute in declaration order") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)

                val (entityId, _) = createEntity(client, observer, log, "OrderTarget", "#000000")

                // Two mutations on the same field — last declared should win
                val set = OperationSet()
                    .add(Operation().entity(entityId).entityType("Node").action(OperationType.MUTATE)
                        .delta(JsonObject().put("color", "#111111")))
                    .add(Operation().entity(entityId).entityType("Node").action(OperationType.MUTATE)
                        .delta(JsonObject().put("color", "#222222")))

                log.step("submitting ordered set", "setId" to set.id)
                submitSet(client, connectionId, set)

                awaitEntityUpdate(client, observer, log, "first mutation") { id, _ -> id == entityId }
                awaitEntityUpdate(client, observer, log, "second mutation") { id, _ -> id == entityId }

                val stored = stateStore.findOneJson(entityId)
                assertEquals("#222222", stored!!.getJsonObject("state")?.getString("color")) {
                    "Second operation (index 1) should win — expected #222222, got: ${stored.getJsonObject("state")?.getString("color")}"
                }

                log.step("order verified", "finalColor" to stored.getJsonObject("state")?.getString("color"))

            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        // ── FAILURE POLICY ────────────────────────────────────────────────────────

        runner.test("SET ABORT: remaining steps skipped after failure") { _ ->
            throw UnsupportedOperationException("Not yet implemented — requires worker-side FailurePolicy execution")
        }

        runner.test("SET ROLLBACK: applied deltas reversed after failure") { _ ->
            throw UnsupportedOperationException("Not yet implemented — requires worker-side FailurePolicy execution")
        }

        // ── STEP TYPES ────────────────────────────────────────────────────────────

        runner.test("FunctionCall: result feeds pipe to next step") { _ ->
            throw UnsupportedOperationException("Not yet implemented — requires worker-side FunctionCall execution")
        }

        runner.test("Assert: pipeline continues on pass") { _ ->
            throw UnsupportedOperationException("Not yet implemented — requires worker-side Assert execution")
        }

        runner.test("Assert: FailurePolicy applied on failure") { _ ->
            throw UnsupportedOperationException("Not yet implemented — requires worker-side Assert execution")
        }

        runner.test("Trigger: pipeline does not wait") { _ ->
            throw UnsupportedOperationException("Not yet implemented — requires worker-side Trigger execution")
        }
    }

    // ── Helpers ───────────────────────────────────────────────────────────────────

    private suspend fun connectClient(client: TestClient, log: TestStepLog): String {
        log.step("connecting")
        val connectionId = client.connect()
        log.step("connected", "connectionId" to connectionId)
        return connectionId
    }

    private suspend fun createEntity(
        client: TestClient,
        observer: OperationObserver,
        log: TestStepLog,
        label: String,
        color: String
    ): Pair<String, JsonObject> {
        log.step("sending CREATE", "label" to label)
        client.send(JsonObject()
            .put("command", "create")
            .put("entity", JsonObject()
                .put("type", "Node")
                .put("state", JsonObject().put("label", label).put("color", color))
            )
        )
        return awaitEntityUpdate(client, observer, log, "CREATE broadcast ($label)") { _, u ->
            u.getString("type") == "Node" && u.getJsonObject("delta")?.getString("label") == label
        }.also { (entityId, _) -> log.step("entity created", "entityId" to entityId) }
    }

    /**
     * Submit all operations in a set as individual command: "operation" messages.
     * Each carries the full operation payload including operationSetId and setIndex.
     *
     * Requires a server-side handler for command: "operation" that submits the payload
     * directly to the coordinator, preserving operationSetId and setIndex.
     */
    private fun submitSet(client: TestClient, connectionId: String, set: OperationSet) {
        val ops: JsonArray = set.toJsonArray()
        for (i in 0 until ops.size()) {
            client.send(JsonObject()
                .put("command", "operation")
                .put("connectionId", connectionId)
                .put("operation", ops.getJsonObject(i))
            )
        }
    }

    private suspend fun awaitEntityUpdate(
        client: TestClient,
        observer: OperationObserver,
        log: TestStepLog,
        stage: String,
        timeoutMs: Long = 5000,
        predicate: (entityId: String, update: JsonObject) -> Boolean
    ): Pair<String, JsonObject> {
        return try {
            client.waitForEntityUpdate(timeoutMs, predicate)
        } catch (e: TimeoutCancellationException) {
            log.caught("timeout at [$stage]", e)
            observer.diagnose(operationId = null, log = log)
            throw Exception("Timed out at stage [$stage] after ${timeoutMs}ms")
        }
    }
}
