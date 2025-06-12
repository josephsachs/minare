/**import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import com.google.inject.Injector

class EntityTestFixtures {
    fun createTestFixture(injector: Injector): Region {


        var regionObj = injector.getInstance(Region::class.java).apply {
            _id = "507f1f77bcf86cd799439011"
            name = "Test Region Alpha"
            zones = ArrayList()
        }

        val zone1 = injector.getInstance(Zone::class.java).apply {
            _id = "507f1f77bcf86cd799439012"
            name = "Combat Zone"
            region = regionObj
            units = ArrayList()
            buildings = ArrayList()
        }

        val zone2 = injector.getInstance(Zone::class.java).apply {
            _id = "507f1f77bcf86cd799439013"
            name = "Safe Zone"
            region = regionObj
            units = ArrayList()
            buildings = ArrayList()
        }

        val hq = injector.getInstance(Building::class.java).apply {
            _id = "507f1f77bcf86cd799439014"
            name = "Headquarters"
            zone = zone1
            position = injector.getInstance(MapVector2::class.java).apply {
                _id = "507f1f77bcf86cd799439018"
                x = 100.0
                y = 100.0
            }
            statuses = HashSet<String>()
            statuses.add("fortified")
            statuses.add("powered")
        }
        hq.position.parentEntity = hq

        val barracks = injector.getInstance(Building::class.java).apply {
            _id = "507f1f77bcf86cd799439015"
            name = "Barracks"
            zone = zone1
            position = injector.getInstance(MapVector2::class.java).apply {
                _id = "507f1f77bcf86cd799439019"
                x = 150.0
                y = 100.0
            }
            statuses = HashSet()
        }
        barracks.position.parentEntity = barracks

        val soldier = injector.getInstance(MapUnit::class.java).apply {
            _id = "507f1f77bcf86cd799439016"
            name = "Elite Soldier"
            zone = zone1
            position = injector.getInstance(MapVector2::class.java).apply {
                _id = "507f1f77bcf86cd799439020"
                x = 120.0
                y = 120.0
            }
            statuses = HashSet()
            statuses.add("alert")
            statuses.add("armed")
            offense = JsonObject()
                .put("burn", true)
                .put("AP", false)
                .put("melee", true)
                .put("damage", JsonArray().add(5).add(10).add(15))
        }
        soldier.position.parentEntity = soldier

        val medic = injector.getInstance(MapUnit::class.java).apply {
            _id = "507f1f77bcf86cd799439017"
            name = "Field Medic"
            zone = zone2
            position = injector.getInstance(MapVector2::class.java).apply {
                _id = "507f1f77bcf86cd799439021"
                x = 200.0
                y = 200.0
            }
            statuses = HashSet()
            statuses.add("healing")
            offense = JsonObject()
                .put("burn", false)
                .put("AP", false)
                .put("melee", false)
                .put("damage", JsonArray().add(1))
        }
        medic.position.parentEntity = medic


        zone1.buildings.add(hq)
        zone1.buildings.add(barracks)
        zone1.units.add(soldier)
        zone2.units.add(medic)
        regionObj.zones.add(zone1)
        regionObj.zones.add(zone2)

        return regionObj
    }


    fun createTestJson(): JsonArray {
        val documents = JsonArray()


        documents.add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439011")
                .put("type", "Region")
                .put("version", 1)
                .put(
                    "state", JsonObject()
                        .put("name", "Test Region Alpha")
                        .put(
                            "zones", JsonArray()
                                .add(
                                    JsonObject()
                                        .put("\$ref", "entity")
                                        .put("\$id", "507f1f77bcf86cd799439012")
                                )
                                .add(
                                    JsonObject()
                                        .put("\$ref", "entity")
                                        .put("\$id", "507f1f77bcf86cd799439013")
                                )
                        )
                )
        )


        documents.add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439013")
                .put("type", "Zone")
                .put("version", 1)
                .put(
                    "state", JsonObject()
                        .put("buildings", JsonArray())
                        .put("name", "Safe Zone")
                        .put(
                            "region", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439011")
                        )
                        .put(
                            "units", JsonArray()
                                .add(
                                    JsonObject()
                                        .put("\$ref", "entity")
                                        .put("\$id", "507f1f77bcf86cd799439017")
                                )
                        )
                )
        )


        documents.add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439017")
                .put("type", "MapUnit")
                .put("version", 1)
                .put(
                    "state", JsonObject()
                        .put("name", "Field Medic")
                        .put(
                            "offense", JsonObject()
                                .put("burn", false)
                                .put("AP", false)
                                .put("melee", false)
                                .put("damage", JsonArray().add(1))
                        )
                        .put(
                            "position", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439021")
                        )
                        .put("statuses", JsonArray().add("healing"))
                        .put(
                            "zone", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439013")
                        )
                )
        )


        documents.add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439021")
                .put("type", "MapVector2")
                .put("version", 1)
                .put(
                    "state", JsonObject()
                        .put(
                            "parentEntity", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439017")
                        )
                        .put("x", 200.0)
                        .put("y", 200.0)
                )
        )


        documents.add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439012")
                .put("type", "Zone")
                .put("version", 1)
                .put(
                    "state", JsonObject()
                        .put(
                            "buildings", JsonArray()
                                .add(
                                    JsonObject()
                                        .put("\$ref", "entity")
                                        .put("\$id", "507f1f77bcf86cd799439014")
                                )
                                .add(
                                    JsonObject()
                                        .put("\$ref", "entity")
                                        .put("\$id", "507f1f77bcf86cd799439015")
                                )
                        )
                        .put("name", "Combat Zone")
                        .put(
                            "region", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439011")
                        )
                        .put(
                            "units", JsonArray()
                                .add(
                                    JsonObject()
                                        .put("\$ref", "entity")
                                        .put("\$id", "507f1f77bcf86cd799439016")
                                )
                        )
                )
        )


        documents.add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439015")
                .put("type", "Building")
                .put("version", 1)
                .put(
                    "state", JsonObject()
                        .put("name", "Barracks")
                        .put(
                            "position", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439019")
                        )
                        .put("statuses", JsonArray())
                        .put(
                            "zone", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439012")
                        )
                )
        )


        documents.add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439019")
                .put("type", "MapVector2")
                .put("version", 1)
                .put(
                    "state", JsonObject()
                        .put(
                            "parentEntity", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439015")
                        )
                        .put("x", 150.0)
                        .put("y", 100.0)
                )
        )


        documents.add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439014")
                .put("type", "Building")
                .put("version", 1)
                .put(
                    "state", JsonObject()
                        .put("name", "Headquarters")
                        .put(
                            "position", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439018")
                        )
                        .put(
                            "statuses", JsonArray()
                                .add("powered")
                                .add("fortified")
                        )
                        .put(
                            "zone", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439012")
                        )
                )
        )


        documents.add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439018")
                .put("type", "MapVector2")
                .put("version", 1)
                .put(
                    "state", JsonObject()
                        .put(
                            "parentEntity", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439014")
                        )
                        .put("x", 100.0)
                        .put("y", 100.0)
                )
        )


        documents.add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439016")
                .put("type", "MapUnit")
                .put("version", 1)
                .put(
                    "state", JsonObject()
                        .put("name", "Elite Soldier")
                        .put(
                            "offense", JsonObject()
                                .put("burn", true)
                                .put("AP", false)
                                .put("melee", true)
                                .put("damage", JsonArray().add(5).add(10).add(15))
                        )
                        .put(
                            "position", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439020")
                        )
                        .put(
                            "statuses", JsonArray()
                                .add("alert")
                                .add("armed")
                        )
                        .put(
                            "zone", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439012")
                        )
                )
        )


        documents.add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439020")
                .put("type", "MapVector2")
                .put("version", 1)
                .put(
                    "state", JsonObject()
                        .put(
                            "parentEntity", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439016")
                        )
                        .put("x", 120.0)
                        .put("y", 120.0)
                )
        )

        return documents
    }

    fun createMockAggregationResults(): JsonArray {
        return JsonArray().add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439020") // MapVector2 (soldier's position)
                .put("type", "MapVector2")
                .put("version", 1)
                .put(
                    "state", JsonObject()
                        .put(
                            "parentEntity", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439016")
                        )
                        .put("x", 120.0)
                        .put("y", 120.0)
                )
                .put(
                    "ancestors", JsonArray()
                        .add(
                            JsonObject() // First ancestor: the soldier
                                .put("_id", "507f1f77bcf86cd799439016")
                                .put("type", "MapUnit")
                                .put("version", 1)
                                .put("depth", 0)
                                .put(
                                    "state", JsonObject()
                                        .put("name", "Elite Soldier")
                                        .put(
                                            "zone", JsonObject()
                                                .put("\$ref", "entity")
                                                .put("\$id", "507f1f77bcf86cd799439012")
                                        ) // Refers to combat zone
                                        .put(
                                            "position", JsonObject()
                                                .put("\$ref", "entity")
                                                .put("\$id", "507f1f77bcf86cd799439020")
                                        ) // Refers back to vector
                                        .put("statuses", JsonArray().add("alert").add("armed"))
                                        .put(
                                            "offense", JsonObject()
                                                .put("burn", true)
                                                .put("AP", false)
                                                .put("melee", true)
                                                .put("damage", JsonArray().add(5).add(10).add(15))
                                        )
                                )
                        )
                        .add(
                            JsonObject() // Second ancestor: combat zone
                                .put("_id", "507f1f77bcf86cd799439012")
                                .put("type", "Zone")
                                .put("version", 1)
                                .put("depth", 1)
                                .put(
                                    "state", JsonObject()
                                        .put("name", "Combat Zone")
                                        .put(
                                            "region", JsonObject()
                                                .put("\$ref", "entity")
                                                .put("\$id", "507f1f77bcf86cd799439011")
                                        ) // Refers to region
                                        .put(
                                            "units", JsonArray()
                                                .add(
                                                    JsonObject()
                                                        .put("\$ref", "entity")
                                                        .put("\$id", "507f1f77bcf86cd799439016")
                                                )
                                        ) // Refers to soldier
                                        .put(
                                            "buildings", JsonArray()
                                                .add(
                                                    JsonObject()
                                                        .put("\$ref", "entity")
                                                        .put("\$id", "507f1f77bcf86cd799439014")
                                                ) // HQ
                                                .add(
                                                    JsonObject()
                                                        .put("\$ref", "entity")
                                                        .put("\$id", "507f1f77bcf86cd799439015")
                                                )
                                        )
                                )
                        ) // Barracks
                        .add(
                            JsonObject() // Third ancestor: region
                                .put("_id", "507f1f77bcf86cd799439011")
                                .put("type", "Region")
                                .put("version", 1)
                                .put("depth", 2)
                                .put(
                                    "state", JsonObject()
                                        .put("name", "Test Region Alpha")
                                        .put(
                                            "zones", JsonArray()
                                                .add(
                                                    JsonObject()
                                                        .put("\$ref", "entity")
                                                        .put("\$id", "507f1f77bcf86cd799439012")
                                                ) // Combat Zone
                                                .add(
                                                    JsonObject()
                                                        .put("\$ref", "entity")
                                                        .put("\$id", "507f1f77bcf86cd799439013")
                                                )
                                        )
                                )
                        ) // Safe Zone
                        // Add buildings that reference zones but aren't in the parent reference chain
                        .add(
                            JsonObject() // Building: HQ
                                .put("_id", "507f1f77bcf86cd799439014")
                                .put("type", "Building")
                                .put("version", 1)
                                .put("depth", 1) // Same depth as zone to make it seem like a sibling
                                .put(
                                    "state", JsonObject()
                                        .put("name", "Headquarters")
                                        .put(
                                            "zone", JsonObject() // References combat zone but not as a parent reference
                                                .put("\$ref", "entity")
                                                .put("\$id", "507f1f77bcf86cd799439012")
                                        )
                                        .put(
                                            "statuses", JsonArray()
                                                .add("powered")
                                                .add("fortified")
                                        )
                                )
                        )
                        .add(
                            JsonObject() // Building: Barracks
                                .put("_id", "507f1f77bcf86cd799439015")
                                .put("type", "Building")
                                .put("version", 1)
                                .put("depth", 1)
                                .put(
                                    "state", JsonObject()
                                        .put("name", "Barracks")
                                        .put(
                                            "zone", JsonObject() // References combat zone but not as a parent reference
                                                .put("\$ref", "entity")
                                                .put("\$id", "507f1f77bcf86cd799439012")
                                        )
                                        .put("statuses", JsonArray())
                                )
                        )
                )
        )
    }

    fun createExpectedAncestorTraversalQuery(entityId: String, collection: String, parentRefPaths: List<String>): JsonObject {
        // Determine the connectToField value
        val connectToField = "state.*"

        val pipeline = JsonArray()
            // Stage 1: Match the starting entity
            .add(JsonObject()
                .put("\$match", JsonObject()
                    .put("_id", entityId)))

            // Stage 2: Use $graphLookup to find all potential ancestors
            .add(JsonObject()
                .put("\$graphLookup", JsonObject()
                    .put("from", collection)
                    .put("startWith", "\$_id")
                    .put("connectFromField", "_id")
                    .put("connectToField", connectToField)
                    .put("as", "ancestors")
                    .put("maxDepth", 10)
                    .put("depthField", "depth")))

        return JsonObject().put("pipeline", pipeline)
    }

    fun createUpdateQuery(): JsonArray {
        return JsonArray()
    }

    // Function to generate test cases for MapVector2 mutations
    fun createMapVector2MutationTestCases(): List<MutationTestCase> {
        val testCases = mutableListOf<MutationTestCase>()

        // Case 1: Valid mutation with only mutable fields
        testCases.add(
            MutationTestCase(
                inputDelta = JsonObject()
                    .put("x", 150.0)
                    .put("y", 200.0),
                expectedPrunedDelta = JsonObject()
                    .put("x", 150.0)
                    .put("y", 200.0)
            )
        )

        // Case 2: Mixed valid and invalid fields
        testCases.add(
            MutationTestCase(
                inputDelta = JsonObject()
                    .put("x", 150.0)
                    .put("parentEntity", "not_allowed")  // Cannot mutate parent reference
                    .put("nonExistentField", "should_be_removed"),
                expectedPrunedDelta = JsonObject()
                    .put("x", 150.0)  // Only x should remain
            )
        )

        // Case 3: None of the fields are mutable
        testCases.add(
            MutationTestCase(
                inputDelta = JsonObject()
                    .put("parentEntity", "not_allowed")
                    .put("_id", "cannot_change_id")
                    .put("type", "cannot_change_type"),
                expectedPrunedDelta = JsonObject()  // Should be empty after pruning
            )
        )

        // Case 4: Nested JSON in mutable fields (if supported)
        testCases.add(
            MutationTestCase(
                inputDelta = JsonObject()
                    .put("x", JsonObject().put("nested", "value"))  // Invalid type for x
                    .put("y", 100.0),
                expectedPrunedDelta = JsonObject()
                    .put("y", 100.0)  // Only y should remain
            )
        )

        return testCases
    }

    data class MutationTestCase(
        val inputDelta: JsonObject,  // The client-sent mutation delta
        val expectedPrunedDelta: JsonObject  // The filtered delta that should be passed to entityStore
    )

    fun createConsistencyLevelTestCases(): List<ConsistencyLevelTestCase> {
        return listOf(
            // Case 1: STRICT fields with matching version - all should pass
            ConsistencyLevelTestCase(
                entityType = "MapVector2",
                inputDelta = JsonObject()
                    .put("x", 150.0)
                    .put("y", 200.0),
                requestedVersion = 5, // Matches entity version
                expectedFilteredDelta = JsonObject()
                    .put("x", 150.0)
                    .put("y", 200.0)
            ),

            // Case 2: STRICT fields with mismatched version - should reject all
            ConsistencyLevelTestCase(
                entityType = "MapVector2",
                inputDelta = JsonObject()
                    .put("x", 150.0)
                    .put("y", 200.0),
                requestedVersion = 4, // Older than entity version
                expectedFilteredDelta = JsonObject() // Empty result
            ),

            // Case 3: PESSIMISTIC fields with older version - should skip
            ConsistencyLevelTestCase(
                entityType = "MapUnit",
                inputDelta = JsonObject()
                    .put("statuses", JsonArray().add("wounded").add("retreating")),
                requestedVersion = 4, // Older than entity version
                expectedFilteredDelta = JsonObject() // Empty result, skip PESSIMISTIC
            ),

            // Case 4: PESSIMISTIC fields with matching version - should include
            ConsistencyLevelTestCase(
                entityType = "MapUnit",
                inputDelta = JsonObject()
                    .put("statuses", JsonArray().add("wounded").add("retreating")),
                requestedVersion = 5, // Matches entity version
                expectedFilteredDelta = JsonObject()
                    .put("statuses", JsonArray().add("wounded").add("retreating"))
            ),

            // Case 5: PESSIMISTIC fields with newer version - should include
            ConsistencyLevelTestCase(
                entityType = "MapUnit",
                inputDelta = JsonObject()
                    .put("statuses", JsonArray().add("wounded").add("retreating")),
                requestedVersion = 6, // Newer than entity version
                expectedFilteredDelta = JsonObject()
                    .put("statuses", JsonArray().add("wounded").add("retreating"))
            ),

            // Case 6: Mixed fields with STRICT violation - should reject all
            ConsistencyLevelTestCase(
                entityType = "MapVector2",
                inputDelta = JsonObject()
                    .put("x", 150.0) // STRICT field
                    .put("y", 200.0), // STRICT field
                requestedVersion = 6, // Different from entity version
                expectedFilteredDelta = JsonObject() // Empty due to STRICT
            )
        )
    }

    // Data class for consistency level test cases
    data class ConsistencyLevelTestCase(
        val entityType: String,
        val inputDelta: JsonObject,
        val requestedVersion: Long,
        val expectedFilteredDelta: JsonObject
    )
}

**/