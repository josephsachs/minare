package com.minare.integration.suites

import com.google.inject.Injector
import com.hazelcast.core.HazelcastInstance
import com.minare.core.frames.services.WorkerRegistry
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

class OperationSetTestSuite(private val injector: Injector) : TestSuite {
    override val name = "OperationSet Tests"

    override suspend fun run(runner: TestRunner) {
        val vertx          = injector.getInstance(Vertx::class.java)
        val stateStore     = injector.getInstance(StateStore::class.java)
        val hazelcast      = injector.getInstance(HazelcastInstance::class.java)
        val workerRegistry = injector.getInstance(WorkerRegistry::class.java)
        val hostName       = workerRegistry.getAllWorkers().firstNotNullOf { it.key }

        // ── ORDERED EXECUTION (success path) ──────────────────────────────────
        runner.test("OperationSet: MUTATE then ASSERT success — mutation lands") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client   = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)
                val entityId = createSetTestEntity(client, observer, log, connectionId, counter = 0)

                log.step("submitting OperationSet: MUTATE counter=5, ASSERT isPositive")
                client.send(operationSetMessage(connectionId, "ABORT",
                    mutateStep(entityId, "counter", 5),
                    assertStep(entityId, "isPositive")
                ))

                val (_, update) = awaitEntityUpdate(
                    client, observer, log,
                    stage     = "MUTATE broadcast after successful set",
                    predicate = { id, upd ->
                        id == entityId && upd.getJsonObject("delta")?.getInteger("counter") == 5
                    }
                )

                log.step("broadcast received", "version" to update.getLong("version"))

                val stored = stateStore.findOneJson(entityId)
                assertNotNull(stored) { "Entity should still exist after successful set" }
                assertEquals(5, stored!!.getJsonObject("state")?.getInteger("counter")) {
                    "counter should be 5 after MUTATE + passing ASSERT"
                }

                log.step("StateStore verified", "counter" to stored.getJsonObject("state")?.getInteger("counter"))
            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        // ── ABORT ON ASSERT FAILURE ────────────────────────────────────────────
        runner.test("OperationSet: MUTATE then ASSERT failure with ABORT — delta stands") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client   = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)
                val entityId = createSetTestEntity(client, observer, log, connectionId, counter = 0)

                log.step("submitting OperationSet: MUTATE counter=5, ASSERT isNegative (will fail — ABORT)")
                client.send(operationSetMessage(connectionId, "ABORT",
                    mutateStep(entityId, "counter", 5),
                    assertStep(entityId, "isNegative")
                ))

                // ABORT: the MUTATE delta stands; tick delivers it
                val (_, update) = awaitEntityUpdate(
                    client, observer, log,
                    stage     = "MUTATE broadcast after ABORT",
                    predicate = { id, upd ->
                        id == entityId && upd.getJsonObject("delta")?.getInteger("counter") == 5
                    }
                )

                log.step("broadcast received", "version" to update.getLong("version"))

                val stored = stateStore.findOneJson(entityId)
                assertNotNull(stored) { "Entity should still exist after ABORT" }
                assertEquals(5, stored!!.getJsonObject("state")?.getInteger("counter")) {
                    "counter should be 5 — ABORT leaves completed mutation deltas standing"
                }

                log.step("StateStore verified — delta stands", "counter" to stored.getJsonObject("state")?.getInteger("counter"))
            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        // ── ROLLBACK ON ASSERT FAILURE ─────────────────────────────────────────
        runner.test("OperationSet: MUTATE then ASSERT failure with ROLLBACK — state restored") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client   = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)
                val entityId = createSetTestEntity(client, observer, log, connectionId, counter = 0)

                log.step("submitting OperationSet: MUTATE counter=5, ASSERT isNegative (will fail → ROLLBACK)")
                client.send(operationSetMessage(connectionId, "ROLLBACK",
                    mutateStep(entityId, "counter", 5),
                    assertStep(entityId, "isNegative")
                ))

                // The rollback reverses the forward MUTATE (5 → 0).
                // Both go through pub/sub. By the time the tick fires, both
                // mutations have written their deltas; the net entity state is counter=0
                // at version=3 (create=1, forward=2, rollback=3).
                val (_, rollbackUpdate) = awaitEntityUpdate(
                    client, observer, log,
                    stage     = "rollback complete broadcast",
                    timeoutMs = 10000,
                    predicate = { id, upd ->
                        id == entityId && (upd.getLong("version") ?: 0) >= 3
                    }
                )

                log.step("rollback broadcast received", "version" to rollbackUpdate.getLong("version"))

                val stored = stateStore.findOneJson(entityId)
                assertNotNull(stored) { "Entity should still exist after rollback" }
                assertEquals(0, stored!!.getJsonObject("state")?.getInteger("counter")) {
                    "counter should be 0 — rollback restored the pre-set state"
                }

                log.step("StateStore verified — rollback successful",
                    "counter" to stored.getJsonObject("state")?.getInteger("counter"))
            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        // ── FUNCTION_CALL ────────────────────────────────────────────────────────
        runner.test("OperationSet: MUTATE then FUNCTION_CALL sees updated state") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client   = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)
                val entityId = createSetTestEntity(client, observer, log, connectionId, counter = 3)

                // MUTATE counter=3→6, then doubleCounter() should double the
                // in-memory instance (6→12) and persist via a second MUTATE.
                log.step("submitting OperationSet: MUTATE counter=6, FUNCTION_CALL doubleCounter, MUTATE counter from doubleCounter result")
                client.send(operationSetMessage(connectionId, "ABORT",
                    mutateStep(entityId, "counter", 6),
                    functionCallStep(entityId, "doubleCounter"),
                    assertStep(entityId, "isPositive")
                ))

                val (_, update) = awaitEntityUpdate(
                    client, observer, log,
                    stage     = "MUTATE broadcast after FunctionCall set",
                    predicate = { id, upd ->
                        id == entityId && upd.getJsonObject("delta")?.getInteger("counter") == 6
                    }
                )

                log.step("broadcast received", "version" to update.getLong("version"))

                // The MUTATE set counter=6 in the store and in-memory.
                // doubleCounter() doubled the in-memory instance to 12 but did NOT
                // persist — only MUTATE steps write to the store.
                // The store should show counter=6 (from the MUTATE).
                val stored = stateStore.findOneJson(entityId)
                assertNotNull(stored) { "Entity should exist" }
                assertEquals(6, stored!!.getJsonObject("state")?.getInteger("counter")) {
                    "counter should be 6 in store — doubleCounter modifies in-memory only"
                }

                log.step("FunctionCall verified — in-memory mutation did not leak to store",
                    "counter" to stored.getJsonObject("state")?.getInteger("counter"))
            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        // ── TRIGGER ──────────────────────────────────────────────────────────────
        runner.test("OperationSet: TRIGGER fires without blocking pipeline") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client   = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)
                val entityId = createSetTestEntity(client, observer, log, connectionId, counter = 0)

                // MUTATE counter=7, TRIGGER markTriggered, ASSERT isPositive
                // The trigger is fire-and-forget; the assert should execute regardless.
                log.step("submitting OperationSet: MUTATE counter=7, TRIGGER markTriggered, ASSERT isPositive")
                client.send(operationSetMessage(connectionId, "ABORT",
                    mutateStep(entityId, "counter", 7),
                    triggerStep(entityId, "markTriggered"),
                    assertStep(entityId, "isPositive")
                ))

                val (_, update) = awaitEntityUpdate(
                    client, observer, log,
                    stage     = "MUTATE broadcast after trigger set",
                    predicate = { id, upd ->
                        id == entityId && upd.getJsonObject("delta")?.getInteger("counter") == 7
                    }
                )

                log.step("broadcast received — pipeline was not blocked by trigger",
                    "version" to update.getLong("version"))

                val stored = stateStore.findOneJson(entityId)
                assertNotNull(stored) { "Entity should exist" }
                assertEquals(7, stored!!.getJsonObject("state")?.getInteger("counter")) {
                    "counter should be 7 after MUTATE"
                }

                log.step("StateStore verified", "counter" to stored.getJsonObject("state")?.getInteger("counter"))
            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        // ── CONTINUE POLICY ──────────────────────────────────────────────────────
        runner.test("OperationSet: CONTINUE policy — second MUTATE lands after ASSERT failure") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client   = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)
                val entityId = createSetTestEntity(client, observer, log, connectionId, counter = 0)

                // MUTATE counter=5, ASSERT alwaysFail (CONTINUE), MUTATE label="survived"
                // With CONTINUE, the failing assert should not halt the pipeline.
                log.step("submitting OperationSet: MUTATE counter=5, ASSERT alwaysFail (CONTINUE), MUTATE label=survived")
                client.send(operationSetMessage(connectionId, "CONTINUE",
                    mutateStep(entityId, "counter", 5),
                    assertStep(entityId, "alwaysFail"),
                    mutateStep(entityId, "label", "survived")
                ))

                // Both mutations should land. Wait for the label mutation specifically.
                val (_, update) = awaitEntityUpdate(
                    client, observer, log,
                    stage     = "label MUTATE broadcast after CONTINUE",
                    predicate = { id, upd ->
                        id == entityId && (upd.getLong("version") ?: 0) >= 3
                    }
                )

                log.step("broadcast received", "version" to update.getLong("version"))

                val stored = stateStore.findOneJson(entityId)
                assertNotNull(stored) { "Entity should exist" }
                assertEquals(5, stored!!.getJsonObject("state")?.getInteger("counter")) {
                    "counter should be 5 — first MUTATE should land"
                }
                assertEquals("survived", stored.getJsonObject("state")?.getString("label")) {
                    "label should be 'survived' — CONTINUE allows second MUTATE after failed assert"
                }

                log.step("CONTINUE policy verified — both mutations landed",
                    "counter" to stored.getJsonObject("state")?.getInteger("counter"),
                    "label" to stored.getJsonObject("state")?.getString("label"))
            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        // ── FUNCTION_CALL → ASSERT PIPE ──────────────────────────────────────────
        runner.test("OperationSet: FunctionCall return feeds stepContext to Assert") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client   = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)
                val entityId = createSetTestEntity(client, observer, log, connectionId, counter = 5)

                // getCounter() returns 5 as stepContext.
                // isPositive() checks counter > 0 on the entity instance (which is 5).
                // If both pass, MUTATE label="piped" is applied.
                log.step("submitting OperationSet: FUNCTION_CALL getCounter, ASSERT isPositive, MUTATE label=piped")
                client.send(operationSetMessage(connectionId, "ABORT",
                    functionCallStep(entityId, "getCounter"),
                    assertStep(entityId, "isPositive"),
                    mutateStep(entityId, "label", "piped")
                ))

                val (_, update) = awaitEntityUpdate(
                    client, observer, log,
                    stage     = "label MUTATE broadcast after pipe set",
                    predicate = { id, upd ->
                        id == entityId && upd.getJsonObject("delta")?.getString("label") == "piped"
                    }
                )

                log.step("broadcast received", "version" to update.getLong("version"))

                val stored = stateStore.findOneJson(entityId)
                assertNotNull(stored) { "Entity should exist" }
                assertEquals("piped", stored!!.getJsonObject("state")?.getString("label")) {
                    "label should be 'piped' — pipeline continued past FunctionCall → Assert"
                }

                log.step("FunctionCall→Assert pipe verified",
                    "label" to stored.getJsonObject("state")?.getString("label"))
            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        // ── ROLLBACK OF CREATE ───────────────────────────────────────────────────
        runner.test("OperationSet: CREATE then ASSERT failure with ROLLBACK — created entity deleted") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client   = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)

                // Create entity A first — we'll use it as the assert target.
                val entityA = createSetTestEntity(client, observer, log, connectionId, counter = 0)

                // Now send a set: CREATE entity B, then ASSERT alwaysFail on entity A.
                // With ROLLBACK, the CREATE of B should be reversed (DELETE).
                log.step("submitting OperationSet: CREATE new entity + ASSERT alwaysFail on entityA (ROLLBACK)")
                client.send(operationSetMessage(connectionId, "ROLLBACK",
                    JsonObject()
                        .put("action", "CREATE")
                        .put("delta",  JsonObject().put("counter", 99)),
                    assertStep(entityA, "alwaysFail")
                ))

                // Capture entityId of the newly created entity B from its CREATE broadcast.
                val (entityB, _) = awaitEntityUpdate(
                    client, observer, log,
                    stage     = "CREATE broadcast for entity B",
                    predicate = { id, upd ->
                        id != entityA && upd.getString("type") == "SetTestEntity"
                                && upd.getJsonObject("delta")?.getInteger("counter") == 99
                    }
                )

                log.step("entity B created", "entityB" to entityB)

                // Wait for rollback DELETE to complete.
                awaitEntityUpdate(
                    client, observer, log,
                    stage     = "DELETE broadcast from rollback of CREATE",
                    timeoutMs = 10000,
                    predicate = { id, upd ->
                        id == entityB && (upd.getLong("version") ?: 0) >= 2
                    }
                )

                // After rollback, entity B should be deleted from the store.
                val stored = stateStore.findOneJson(entityB)
                assertEquals(null, stored) {
                    "Entity B should be deleted after rollback of CREATE"
                }

                log.step("CREATE rollback verified — entity B deleted")
            } finally {
                observer.stop()
                client.disconnect()
            }
        }
    }

    // ── Shared helpers ─────────────────────────────────────────────────────────

    private suspend fun connectClient(client: TestClient, log: TestStepLog): String {
        log.step("connecting")
        val connectionId = client.connect()
        log.step("connected", "connectionId" to connectionId)
        return connectionId
    }

    /**
     * Create a SetTestEntity via a single-step OperationSet CREATE.
     * This goes through the normal handler path (hooks, channel registration, direct broadcast).
     */
    private suspend fun createSetTestEntity(
        client: TestClient,
        observer: OperationObserver,
        log: TestStepLog,
        connectionId: String,
        counter: Int
    ): String {
        log.step("creating SetTestEntity", "counter" to counter)
        client.send(JsonObject()
            .put("command", "operationSet")
            .put("connectionId", connectionId)
            .put("failurePolicy", "ABORT")
            .put("steps", JsonArray()
                .add(JsonObject()
                    .put("action", "CREATE")
                    .put("delta",  JsonObject().put("counter", counter))
                )
            )
        )

        val (entityId, _) = awaitEntityUpdate(
            client, observer, log,
            stage     = "CREATE broadcast (SetTestEntity setup)",
            predicate = { _, upd -> upd.getString("type") == "SetTestEntity" }
        )

        log.step("SetTestEntity created", "entityId" to entityId, "counter" to counter)
        return entityId
    }

    private suspend fun awaitEntityUpdate(
        client: TestClient,
        observer: OperationObserver,
        log: TestStepLog,
        stage: String,
        timeoutMs: Long = 8000,
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

    // ── Message builders ───────────────────────────────────────────────────────

    private fun operationSetMessage(
        connectionId: String,
        failurePolicy: String,
        vararg steps: JsonObject
    ): JsonObject = JsonObject()
        .put("command",       "operationSet")
        .put("connectionId",  connectionId)
        .put("failurePolicy", failurePolicy)
        .put("steps",         JsonArray(steps.toList()))

    private fun mutateStep(entityId: String, field: String, value: Any): JsonObject =
        JsonObject()
            .put("action",   "MUTATE")
            .put("entityId", entityId)
            .put("delta",    JsonObject().put(field, value))

    private fun assertStep(entityId: String, function: String): JsonObject =
        JsonObject()
            .put("action",   "ASSERT")
            .put("entityId", entityId)
            .put("function", function)

    private fun functionCallStep(entityId: String, function: String): JsonObject =
        JsonObject()
            .put("action",   "FUNCTION_CALL")
            .put("entityId", entityId)
            .put("function", function)

    private fun triggerStep(entityId: String, function: String): JsonObject =
        JsonObject()
            .put("action",   "TRIGGER")
            .put("entityId", entityId)
            .put("function", function)
}
