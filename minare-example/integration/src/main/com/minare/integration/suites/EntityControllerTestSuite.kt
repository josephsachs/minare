package com.minare.integration.suites

import com.google.inject.Injector
import com.minare.controller.EntityController
import com.minare.core.storage.interfaces.StateStore
import com.minare.integration.harness.Assertions.assertEquals
import com.minare.integration.harness.Assertions.assertNotNull
import com.minare.integration.harness.Assertions.assertNotStartsWith
import com.minare.integration.harness.TestRunner
import com.minare.integration.harness.TestSuite
import com.minare.integration.models.Node
import io.vertx.core.json.JsonObject

class EntityControllerTestSuite(private val injector: Injector) : TestSuite {
    override val name = "EntityController Tests"

    override suspend fun run(runner: TestRunner) {
        val entityController = injector.getInstance(EntityController::class.java)
        val stateStore = injector.getInstance(StateStore::class.java)

        runner.test("create entity assigns ID") {
            val node = Node().apply {
                label = "Test Node"
            }

            val created = entityController.create(node)

            assertNotNull(created._id) { "Created entity should have an ID" }
            assertNotStartsWith(created._id, "unsaved-") { "ID should not be unsaved prefix" }
        }

        runner.test("create entity sets version to 1") {
            val node = Node().apply {
                label = "Version Test"
            }

            val created = entityController.create(node)

            assertEquals(1L, created.version) { "New entity should have version 1" }
        }

        runner.test("created entity can be retrieved from StateStore") {
            val node = Node().apply {
                label = "Retrieval Test"
                color = "#FF0000"
            }

            val created = entityController.create(node)
            val retrieved = stateStore.findEntity(created._id)

            assertNotNull(retrieved) { "Should be able to retrieve created entity" }
            assertEquals(created._id, retrieved!!._id) { "Retrieved ID should match" }
            assertEquals("Node", retrieved.type) { "Type should be Node" }
        }

        runner.test("entity state fields are persisted to Redis") {
            val node = Node().apply {
                label = "State Test"
                color = "#00FF00"
                childIds = mutableListOf("child1", "child2")
            }

            val created = entityController.create(node)
            val json = stateStore.findEntityJson(created._id)

            assertNotNull(json) { "Should retrieve entity JSON" }

            val state = json!!.getJsonObject("state")
            assertNotNull(state) { "JSON should have state object" }
            assertEquals("State Test", state.getString("label")) { "Label should be persisted" }
            assertEquals("#00FF00", state.getString("color")) { "Color should be persisted" }

            val children = state.getJsonArray("childIds")
            assertNotNull(children) { "childIds should be persisted" }
            assertEquals(2, children.size()) { "Should have 2 children" }
        }

        runner.test("saveState updates entity and increments version") {
            val node = Node().apply {
                label = "Update Test"
                color = "#0000FF"
            }

            val created = entityController.create(node)
            assertEquals(1L, created.version) { "Initial version should be 1" }

            val delta = JsonObject().put("color", "#FFFFFF")

            val updated = entityController.saveState(created._id, delta)

            assertNotNull(updated) { "Updated entity should not be null" }
            assertEquals(2L, updated!!.version) { "Version should increment to 2" }

            val json = stateStore.findEntityJson(created._id)
            val state = json!!.getJsonObject("state")
            assertEquals("#FFFFFF", state.getString("color")) { "Color should be updated" }
        }
    }
}