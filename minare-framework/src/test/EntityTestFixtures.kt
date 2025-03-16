import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

class EntityTestFixtures {
    fun createTestFixture(): Region {
        val region = Region()
        region._id = "507f1f77bcf86cd799439011"
        region.name = "Test Region Alpha"
        region.zones = ArrayList()

        val zone1 = Zone()
        zone1._id = "507f1f77bcf86cd799439012"
        zone1.name = "Combat Zone"
        zone1.region = region
        zone1.units = ArrayList()
        zone1.buildings = ArrayList()

        val zone2 = Zone()
        zone2._id = "507f1f77bcf86cd799439013"
        zone2.name = "Safe Zone"
        zone2.region = region
        zone2.units = ArrayList()
        zone2.buildings = ArrayList()

        val hq = Building()
        hq._id = "507f1f77bcf86cd799439014"
        hq.name = "Headquarters"
        hq.zone = zone1
        //hq.position = MapVector2()
        //hq.position._id = "507f1f77bcf86cd799439018"
        //hq.position.x = 100.0
        //hq.position.y = 100.0
        //hq.position.owner = hq
        hq.statuses = HashSet<String>()
        hq.statuses.add("fortified")
        hq.statuses.add("powered")

        val barracks = Building()
        barracks._id = "507f1f77bcf86cd799439015"
        barracks.name = "Barracks"
        barracks.zone = zone1
        //barracks.position = MapVector2()
        //barracks.position._id = "507f1f77bcf86cd799439019"
        //barracks.position.x = 150.0
        //barracks.position.y = 100.0
        //barracks.position.owner = barracks
        barracks.statuses = HashSet()  // empty set

        val soldier = MapUnit()
        soldier._id = "507f1f77bcf86cd799439016"
        soldier.name = "Elite Soldier"
        soldier.zone = zone1
        soldier.position = MapVector2()
        soldier.position._id = "507f1f77bcf86cd799439020"
        soldier.position.x = 120.0
        soldier.position.y = 120.0
        soldier.position.parentEntity = soldier
        soldier.statuses = HashSet()
        soldier.statuses.add("alert")
        soldier.statuses.add("armed")
        soldier.offense = JsonObject()
            .put("burn", true)
            .put("AP", false)
            .put("melee", true)
            .put("damage", JsonArray().add(5).add(10).add(15))

        val medic = MapUnit()
        medic._id = "507f1f77bcf86cd799439017"
        medic.name = "Field Medic"
        medic.zone = zone2  // in safe zone
        medic.position = MapVector2()
        medic.position._id = "507f1f77bcf86cd799439021"
        medic.position.x = 200.0
        medic.position.y = 200.0
        medic.position.parentEntity = medic
        medic.statuses = HashSet()
        medic.statuses.add("healing")
        medic.offense = JsonObject()
            .put("burn", false)
            .put("AP", false)
            .put("melee", false)
            .put("damage", JsonArray().add(1))

        // Wire up all the references
        zone1.buildings.add(hq)
        zone1.buildings.add(barracks)
        zone1.units.add(soldier)
        zone2.units.add(medic)
        region.zones.add(zone1)
        region.zones.add(zone2)

        return region
    }

    // The expected serialized form (would be generated from actual output)
    fun createTestJson(): JsonArray {
        val documents = JsonArray()

        // Root Region
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

        // Safe Zone
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

        // Field Medic
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

        // Medic Position Vector
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

        // Combat Zone
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

        // Barracks
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

        // Headquarters
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

        // Elite Soldier
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

        // Soldier Position Vector
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
        // This represents the results of a MongoDB $graphLookup aggregation
        // starting from the soldier's position vector and finding all ancestors
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
                        ) // Refers to soldier
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
}