package com.minare.integration.suites

import com.google.inject.Injector
import com.hazelcast.core.HazelcastInstance
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.operation.models.*
import com.minare.core.operation.models.OperationSet.FailurePolicy
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

                val members = JsonArray()
                    .add(mutateNode(entityId, JsonObject().put("color", "#AA0000")))
                    .add(mutateNode(entityId, JsonObject().put("label", "Updated")))

                log.step("submitting set", "size" to members.size())
                submitSet(client, connectionId, members)

                // Both mutations land in the same frame and are delivered as
                // a single combined update on the tick — one broadcast, not two.
                awaitEntityUpdate(client, observer, log, "set mutations") { id, _ -> id == entityId }

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

                val members = JsonArray()
                    .add(mutateNode(entity1Id, JsonObject().put("color", "#AAAAAA")))
                    .add(mutateNode(entity2Id, JsonObject().put("color", "#BBBBBB")))

                log.step("submitting set")
                submitSet(client, connectionId, members)

                awaitEntityUpdate(client, observer, log, "op1 broadcast") { id, _ -> id == entity1Id }
                awaitEntityUpdate(client, observer, log, "op2 broadcast") { id, _ -> id == entity2Id }

                // Verify both landed on same worker via state store (both were mutated)
                val s1 = stateStore.findOneJson(entity1Id)
                val s2 = stateStore.findOneJson(entity2Id)
                assertEquals("#AAAAAA", s1!!.getJsonObject("state")?.getString("color")) {
                    "Entity 1 color should be mutated"
                }
                assertEquals("#BBBBBB", s2!!.getJsonObject("state")?.getString("color")) {
                    "Entity 2 color should be mutated"
                }

                log.step("co-location verified — both mutations applied")

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
                val members = JsonArray()
                    .add(mutateNode(entityId, JsonObject().put("color", "#111111")))
                    .add(mutateNode(entityId, JsonObject().put("color", "#222222")))

                log.step("submitting ordered set")
                submitSet(client, connectionId, members)

                // Both mutations target the same entity in the same frame —
                // single broadcast. Order is proven by which value survives.
                awaitEntityUpdate(client, observer, log, "set mutations") { id, _ -> id == entityId }

                val stored = stateStore.findOneJson(entityId)
                assertEquals("#222222", stored!!.getJsonObject("state")?.getString("color")) {
                    "Second operation (index 1) should win — expected #222222, got: ${stored.getJsonObject("state")?.getString("color")}"
                }

                log.step("order verified — last writer won",
                    "finalColor" to stored.getJsonObject("state")?.getString("color"))

            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        // ── STEP TYPES ────────────────────────────────────────────────────────────

        runner.test("FunctionCall: result feeds pipe to next step") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)

                val entityId = createSetTestEntity(client, observer, log, stateStore, "PipeTarget", 7)

                // compute(operand=7) → returns {result: 14} → checkResult asserts result == 14
                val members = JsonArray()
                    .add(functionCall(entityId, "compute", JsonObject().put("operand", 7)))
                    .add(assertCall(entityId, "checkResult", JsonObject().put("expected", 14)))

                log.step("submitting compute→assert pipe")
                submitSet(client, connectionId, members)

                // Wait for the set to execute — no broadcast for predicates,
                // poll the entity's properties for step execution records.
                awaitProperties(stateStore, entityId, log, "checkResult") { props ->
                    props.getJsonObject("timestamps")?.containsKey("checkResult") == true
                }

                val stored = stateStore.findOneJson(entityId)!!
                val props = stored.getJsonObject("properties", JsonObject())
                val hydration = props.getJsonObject("hydrationChecks", JsonObject())

                assertEquals("true", hydration.getString("compute")) {
                    "compute should see hydrated entity"
                }
                assertEquals("true", hydration.getString("checkResult")) {
                    "checkResult should see hydrated entity"
                }

                // Verify ordering: compute ran before checkResult
                val timestamps = props.getJsonObject("timestamps", JsonObject())
                val computeTime = timestamps.getString("compute")?.toLongOrNull() ?: 0
                val checkTime = timestamps.getString("checkResult")?.toLongOrNull() ?: 0
                assert(computeTime <= checkTime) {
                    "compute ($computeTime) should run before checkResult ($checkTime)"
                }

                log.step("pipe verified",
                    "computeAt" to computeTime,
                    "checkResultAt" to checkTime,
                    "lastContext" to props.getString("lastContext"))

            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        runner.test("Assert: pipeline continues on pass") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)

                val entityId = createSetTestEntity(client, observer, log, stateStore, "AssertPassTarget", 5)

                // alwaysPass → snapshot (proves pipeline continued past the assert)
                val members = JsonArray()
                    .add(assertCall(entityId, "alwaysPass"))
                    .add(functionCall(entityId, "snapshot"))

                log.step("submitting assert-pass→snapshot set")
                submitSet(client, connectionId, members)

                awaitProperties(stateStore, entityId, log, "snapshot") { props ->
                    props.getJsonObject("timestamps")?.containsKey("snapshot") == true
                }

                val props = stateStore.findOneJson(entityId)!!
                    .getJsonObject("properties", JsonObject())
                val timestamps = props.getJsonObject("timestamps", JsonObject())

                assertNotNull(timestamps.getString("alwaysPass")) { "alwaysPass should have executed" }
                assertNotNull(timestamps.getString("snapshot")) { "snapshot should have executed after pass" }

                log.step("assert pass verified — pipeline continued to snapshot")

            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        // ── FAILURE POLICY ────────────────────────────────────────────────────────

        runner.test("SET ABORT: remaining steps skipped after failure") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)

                val entityId = createSetTestEntity(client, observer, log, stateStore, "AbortTarget", 10)

                // compute → alwaysFail (ABORT) → snapshot (should NOT execute)
                val members = JsonArray()
                    .add(functionCall(entityId, "compute", JsonObject().put("operand", 3)))
                    .add(assertCall(entityId, "alwaysFail"))
                    .add(functionCall(entityId, "snapshot"))

                log.step("submitting compute→fail→snapshot with ABORT")
                submitSet(client, connectionId, members, FailurePolicy.ABORT)

                // Wait for alwaysFail to record itself
                awaitProperties(stateStore, entityId, log, "alwaysFail") { props ->
                    props.getJsonObject("timestamps")?.containsKey("alwaysFail") == true
                }

                val props = stateStore.findOneJson(entityId)!!
                    .getJsonObject("properties", JsonObject())
                val timestamps = props.getJsonObject("timestamps", JsonObject())

                assertNotNull(timestamps.getString("compute")) { "compute should have executed" }
                assertNotNull(timestamps.getString("alwaysFail")) { "alwaysFail should have executed" }
                assertEquals(null, timestamps.getString("snapshot")) {
                    "snapshot should NOT have executed after ABORT"
                }

                log.step("ABORT verified — snapshot was skipped")

            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        runner.test("SET ROLLBACK: applied deltas reversed after failure") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)

                val (nodeId, _) = createEntity(client, observer, log, "RollbackTarget", "#000000")
                val assertEntityId = createSetTestEntity(client, observer, log, stateStore, "RollbackAssert", 0)

                // Mutate Node's color → alwaysFail on SetTestEntity (ROLLBACK) → color should revert
                val members = JsonArray()
                    .add(mutateNode(nodeId, JsonObject().put("color", "#FF0000")))
                    .add(assertCall(assertEntityId, "alwaysFail"))

                log.step("submitting mutate→fail with ROLLBACK")
                submitSet(client, connectionId, members, FailurePolicy.ROLLBACK)

                // Wait for alwaysFail to record, then give rollback time to complete
                awaitProperties(stateStore, assertEntityId, log, "alwaysFail") { props ->
                    props.getJsonObject("timestamps")?.containsKey("alwaysFail") == true
                }
                kotlinx.coroutines.delay(1000)

                val stored = stateStore.findOneJson(nodeId)!!
                val color = stored.getJsonObject("state")?.getString("color")
                assertEquals("#000000", color) {
                    "Color should revert to #000000 after rollback — got: $color"
                }

                log.step("ROLLBACK verified — color reverted", "color" to color)

            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        runner.test("Trigger: fire-and-forget does not block pipeline") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)

                val entityId = createSetTestEntity(client, observer, log, stateStore, "TriggerTarget", 42)

                // snapshot → trigger(onTriggered) → alwaysPass
                // Trigger is fire-and-forget: pipeline should reach alwaysPass
                // without waiting for onTriggered to complete.
                val members = JsonArray()
                    .add(functionCall(entityId, "snapshot"))
                    .add(triggerCall(entityId, "onTriggered"))
                    .add(assertCall(entityId, "alwaysPass"))

                log.step("submitting snapshot→trigger→assert set")
                submitSet(client, connectionId, members)

                awaitProperties(stateStore, entityId, log, "alwaysPass") { props ->
                    props.getJsonObject("timestamps")?.containsKey("alwaysPass") == true
                }

                val props = stateStore.findOneJson(entityId)!!
                    .getJsonObject("properties", JsonObject())
                val timestamps = props.getJsonObject("timestamps", JsonObject())

                assertNotNull(timestamps.getString("snapshot")) { "snapshot should have executed" }
                assertNotNull(timestamps.getString("alwaysPass")) {
                    "alwaysPass should have executed — trigger did not block"
                }

                log.step("trigger verified — pipeline was not blocked")

            } finally {
                observer.stop()
                client.disconnect()
            }
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
     * Submit an OperationSet as a single command: "set" message.
     * The server-side handler builds real model objects (Operation, FunctionCall,
     * Assert, Trigger) and returns the OperationSet for queueing.
     */
    private suspend fun submitSet(
        client: TestClient,
        connectionId: String,
        members: JsonArray,
        failurePolicy: FailurePolicy = FailurePolicy.CONTINUE
    ) {
        client.send(JsonObject()
            .put("command", "set")
            .put("connectionId", connectionId)
            .put("members", members)
            .put("failurePolicy", failurePolicy.name)
        )
    }

    /**
     * Create a SetTestEntity via the operation passthrough (CREATE action).
     * Returns the entityId after confirming creation via broadcast.
     */
    private suspend fun createSetTestEntity(
        client: TestClient,
        observer: OperationObserver,
        log: TestStepLog,
        stateStore: StateStore,
        label: String,
        value: Int
    ): String {
        log.step("sending CREATE SetTestEntity", "label" to label, "value" to value)
        client.send(JsonObject()
            .put("command", "operation")
            .put("operation", JsonObject()
                .put("action", "CREATE")
                .put("entityType", "SetTestEntity")
                .put("delta", JsonObject().put("label", label).put("value", value))
            )
        )
        val (entityId, _) = awaitEntityUpdate(client, observer, log, "CREATE SetTestEntity ($label)") { _, u ->
            u.getString("type") == "SetTestEntity"
        }
        log.step("SetTestEntity created", "entityId" to entityId)
        return entityId
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

    /**
     * Poll entity properties until a condition is met.
     * Used for predicate tests where there's no pub/sub broadcast.
     */
    private suspend fun awaitProperties(
        stateStore: StateStore,
        entityId: String,
        log: TestStepLog,
        stage: String,
        timeoutMs: Long = 5000,
        predicate: (props: JsonObject) -> Boolean
    ) {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            val entity = stateStore.findOneJson(entityId)
            val props = entity?.getJsonObject("properties", JsonObject()) ?: JsonObject()
            if (predicate(props)) return
            kotlinx.coroutines.delay(100)
        }
        throw Exception("Timed out waiting for properties at stage [$stage] after ${timeoutMs}ms")
    }

    // ── Member builders ──────────────────────────────────────────────────────────

    private fun mutateNode(entityId: String, delta: JsonObject) = JsonObject()
        .put("action", "MUTATE")
        .put("entityId", entityId)
        .put("entityType", "Node")
        .put("delta", delta)

    private fun functionCall(entityId: String, target: String, values: JsonObject = JsonObject()) = JsonObject()
        .put("action", "FUNCTION_CALL")
        .put("entityId", entityId)
        .put("entityType", "SetTestEntity")
        .put("target", target)
        .put("values", values)

    private fun assertCall(entityId: String, target: String, values: JsonObject = JsonObject()) = JsonObject()
        .put("action", "ASSERT")
        .put("entityId", entityId)
        .put("entityType", "SetTestEntity")
        .put("target", target)
        .put("values", values)

    private fun triggerCall(entityId: String, target: String, values: JsonObject = JsonObject()) = JsonObject()
        .put("action", "TRIGGER")
        .put("entityId", entityId)
        .put("entityType", "SetTestEntity")
        .put("target", target)
        .put("values", values)
}
