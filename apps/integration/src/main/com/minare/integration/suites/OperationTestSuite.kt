package com.minare.integration.suites

import com.google.inject.Injector
import com.minare.controller.EntityController
import com.minare.core.frames.services.WorkerRegistry
import com.minare.core.storage.interfaces.StateStore
import com.minare.integration.controller.TestChannelController
import com.minare.integration.harness.Assertions.assertEquals
import com.minare.integration.harness.Assertions.assertNotNull
import com.minare.integration.harness.Assertions.assertNull
import com.minare.integration.harness.TestClient
import com.minare.integration.harness.TestRunner
import com.minare.integration.harness.TestSuite
import io.vertx.core.Vertx
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.core.json.JsonObject

class OperationTestSuite(private val injector: Injector) : TestSuite {
    override val name = "Operation Tests"
    private val log = LoggerFactory.getLogger(OperationTestSuite::class.java)

    override suspend fun run(runner: TestRunner) {
        val vertx = injector.getInstance(Vertx::class.java)
        val stateStore = injector.getInstance(StateStore::class.java)
        val channelController = injector.getInstance(TestChannelController::class.java)
        val entityController = injector.getInstance(EntityController::class.java)

        runner.test("full entity lifecycle: CREATE -> MUTATE -> DELETE") {
            val workerRegistry = injector.getInstance(WorkerRegistry::class.java)
            val hostName = workerRegistry.getAllWorkers().firstNotNullOf { it.key }

            log.info("HOSTNAME_TEST: $hostName")

            val client = TestClient(vertx, hostName, 4225, hostName, 4226)

            try {
                // Connect and subscribe to default channel
                val connectionId = client.connect()

                // === CREATE ===
                val createOp = JsonObject()
                    .put("command", "create")
                    .put("connectionId", connectionId)
                    .put("entity", JsonObject()
                        .put("type", "Node")
                        .put("state", JsonObject()
                            .put("label", "Test Node")
                            .put("color", "#FF0000")
                        )
                    )

                client.send(createOp)

                val (entityId, createUpdate) = client.waitForEntityUpdate { id, update ->
                    update.getString("type") == "Node" &&
                            update.getJsonObject("delta")?.getString("label") == "Test Node"
                }

                assertNotNull(entityId) { "Created entity should have an ID" }
                assertEquals(1L, createUpdate.getLong("version")) { "New entity should have version 1" }

                // Verify in StateStore
                val createdEntity = stateStore.findEntityJson(entityId)
                assertNotNull(createdEntity) { "Entity should exist in StateStore" }
                assertEquals(1L, createdEntity!!.getLong("version")) { "New entity should have version 1" }

                // === MUTATE ===
                val mutateOp = JsonObject()
                    .put("command", "mutate")
                    .put("connectionId", connectionId)
                    .put("entity", JsonObject()
                        .put("_id", entityId)
                        .put("version", 1L)
                        .put("state", JsonObject()
                            .put("color", "#00FF00")
                        )
                    )

                client.send(mutateOp)

                // Wait for mutation update
                val (mutatedEntityId, mutateUpdate) = client.waitForEntityUpdate { id, update ->
                    update.getString("type") == "Node" &&
                            update.getJsonObject("delta")?.getString("label") == "Test Node"
                }

                assertEquals("#00FF00", mutateUpdate.getJsonObject("delta")?.getString("color")) {
                    "Delta should contain color change"
                }

                // Verify in StateStore
                val mutatedEntity = stateStore.findEntityJson(entityId)
                assertEquals(2L, mutatedEntity!!.getLong("version")) { "Version should be 2" }
                assertEquals("#00FF00", mutatedEntity.getJsonObject("state")?.getString("color")) {
                    "Color should be updated"
                }

                // === DELETE ===
                val deleteOp = JsonObject()
                    .put("command", "delete")
                    .put("connectionId", connectionId)
                    .put("entity", JsonObject()
                        .put("_id", entityId)
                    )

                client.send(deleteOp)

                // Wait for delete notification (TBD what this looks like)
                val (deletedEntityId, deleteUpdate) = client.waitForEntityUpdate { id, update ->
                    update.getString("type") == "Node" &&
                            update.getJsonObject("delta")?.getString("label") == "Test Node"
                }

                assertNotNull(deleteUpdate) { "Should receive delete notification" }

                // Verify entity is gone
                val deletedEntity = stateStore.findEntityJson(entityId)
                assertNull(deletedEntity) { "Entity should no longer exist in StateStore" }

            } finally {
                client.disconnect()
            }
        }
    }
}