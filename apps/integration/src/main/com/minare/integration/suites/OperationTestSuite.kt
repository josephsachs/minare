package com.minare.integration.suites

import com.google.inject.Injector
import com.hazelcast.core.HazelcastInstance
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.storage.interfaces.StateStore
import com.minare.integration.harness.Assertions.assertEquals
import com.minare.integration.harness.Assertions.assertNotNull
import com.minare.integration.harness.Assertions.assertNull
import com.minare.integration.harness.OperationObserver
import com.minare.integration.harness.TestClient
import com.minare.integration.harness.TestRunner
import com.minare.integration.harness.TestStepLog
import com.minare.integration.harness.TestSuite
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.TimeoutCancellationException
import org.slf4j.LoggerFactory

class OperationTestSuite(private val injector: Injector) : TestSuite {
    override val name = "Operation Tests"
    private val log = LoggerFactory.getLogger(OperationTestSuite::class.java)

    override suspend fun run(runner: TestRunner) {
        val vertx = injector.getInstance(Vertx::class.java)
        val stateStore = injector.getInstance(StateStore::class.java)
        val hazelcast = injector.getInstance(HazelcastInstance::class.java)
        val workerRegistry = injector.getInstance(WorkerRegistry::class.java)
        val hostName = workerRegistry.getAllWorkers().firstNotNullOf { it.key }

        // ── CREATE ──────────────────────────────────────────────────────────────
        runner.test("CREATE: entity persisted and broadcast received") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)

                log.step("sending CREATE command")
                client.send(JsonObject()
                    .put("command", "create")
                    .put("connectionId", connectionId)
                    .put("entity", JsonObject()
                        .put("type", "Node")
                        .put("state", JsonObject()
                            .put("label", "Test Node")
                            .put("color", "#FF0000")
                        )
                    )
                )

                val (entityId, createUpdate) = awaitEntityUpdate(
                    client, observer, log, stage = "CREATE broadcast",
                    predicate = { _, update ->
                        update.getString("type") == "Node" &&
                        update.getJsonObject("delta")?.getString("label") == "Test Node"
                    }
                )

                log.step("broadcast received", "entityId" to entityId, "version" to createUpdate.getLong("version"))

                assertNotNull(entityId) { "Created entity should have an ID" }
                assertEquals(1L, createUpdate.getLong("version")) { "New entity should have version 1" }

                val stored = stateStore.findOneJson(entityId)
                assertNotNull(stored) { "Entity should exist in StateStore after CREATE" }
                assertEquals(1L, stored!!.getLong("version")) { "StateStore version should be 1" }

                log.step("StateStore verified", "version" to stored.getLong("version"))

            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        // ── MUTATE ──────────────────────────────────────────────────────────────
        runner.test("MUTATE: entity state updated and version advanced") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)

                log.step("sending CREATE (MUTATE setup)")
                client.send(JsonObject()
                    .put("command", "create")
                    .put("connectionId", connectionId)
                    .put("entity", JsonObject()
                        .put("type", "Node")
                        .put("state", JsonObject()
                            .put("label", "Mutate Target")
                            .put("color", "#FF0000")
                        )
                    )
                )

                val (entityId, _) = awaitEntityUpdate(
                    client, observer, log, stage = "CREATE broadcast (MUTATE setup)",
                    predicate = { _, update ->
                        update.getString("type") == "Node" &&
                        update.getJsonObject("delta")?.getString("label") == "Mutate Target"
                    }
                )

                log.step("CREATE complete", "entityId" to entityId)

                log.step("sending MUTATE command", "entityId" to entityId)
                client.send(JsonObject()
                    .put("command", "mutate")
                    .put("connectionId", connectionId)
                    .put("entity", JsonObject()
                        .put("_id", entityId)
                        .put("version", 1L)
                        .put("state", JsonObject()
                            .put("color", "#00FF00")
                        )
                    )
                )

                val (_, mutateUpdate) = awaitEntityUpdate(
                    client, observer, log, stage = "MUTATE broadcast",
                    predicate = { id, _ -> id == entityId }
                )

                log.step("MUTATE broadcast received", "version" to mutateUpdate.getLong("version"))

                assertEquals("#00FF00", mutateUpdate.getJsonObject("delta")?.getString("color")) {
                    "Delta should contain updated color"
                }

                val stored = stateStore.findOneJson(entityId)
                assertEquals(2L, stored!!.getLong("version")) { "Version should be 2 after MUTATE" }
                assertEquals("#00FF00", stored.getJsonObject("state")?.getString("color")) {
                    "Color should be updated in StateStore"
                }

                log.step("StateStore verified", "version" to stored.getLong("version"), "color" to stored.getJsonObject("state")?.getString("color"))

            } finally {
                observer.stop()
                client.disconnect()
            }
        }

        // ── DELETE ──────────────────────────────────────────────────────────────
        runner.test("DELETE: deletion broadcast received and entity removed from store") { log ->
            val observer = OperationObserver(vertx, hazelcast).also { it.start() }
            val client = TestClient(vertx, hostName, 4225, hostName, 4226)
            try {
                val connectionId = connectClient(client, log)

                log.step("sending CREATE (DELETE setup)")
                client.send(JsonObject()
                    .put("command", "create")
                    .put("connectionId", connectionId)
                    .put("entity", JsonObject()
                        .put("type", "Node")
                        .put("state", JsonObject()
                            .put("label", "Delete Target")
                            .put("color", "#0000FF")
                        )
                    )
                )

                val (entityId, _) = awaitEntityUpdate(
                    client, observer, log, stage = "CREATE broadcast (DELETE setup)",
                    predicate = { _, update ->
                        update.getString("type") == "Node" &&
                        update.getJsonObject("delta")?.getString("label") == "Delete Target"
                    }
                )

                log.step("CREATE complete", "entityId" to entityId)

                log.step("sending DELETE command", "entityId" to entityId)
                client.send(JsonObject()
                    .put("command", "delete")
                    .put("connectionId", connectionId)
                    .put("entity", JsonObject()
                        .put("_id", entityId)
                    )
                )

                awaitEntityUpdate(
                    client, observer, log, stage = "DELETE broadcast",
                    predicate = { id, update ->
                        id == entityId && update.getJsonObject("delta")?.getBoolean("_deleted") == true
                    }
                )

                log.step("DELETE broadcast received")

                val deletedEntity = runCatching { stateStore.findOneJson(entityId) }.getOrNull()
                assertNull(deletedEntity) { "Entity should not exist in StateStore after DELETE" }

                log.step("StateStore verified — entity absent")

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
     * Await a downsocket entity update matching the predicate.
     * On timeout, appends server-side frame and operation completion state before rethrowing
     * with a message that names the stage that stalled.
     */
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
            throw TimeoutCancellationException("Timed out at stage [$stage] after ${timeoutMs}ms")
        }
    }
}
