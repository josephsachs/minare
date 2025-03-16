package test.java.java

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonTypeName
import com.minare.core.entity.annotations.*
import com.minare.core.models.Entity
import com.minare.core.entity.EntityFactory
import com.minare.core.entity.EntityReflector
import com.minare.persistence.MongoEntityStore
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.mockito.Mock
import org.mockito.MockitoAnnotations
import io.vertx.ext.mongo.MongoClient
import io.vertx.junit5.VertxExtension
import io.vertx.junit5.VertxTestContext
import kotlinx.coroutines.runBlocking
import java.security.MessageDigest
import org.assertj.core.api.Assertions.assertThat
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mockito

@ExtendWith(VertxExtension::class)
class EntityTest {
    private lateinit var vertx: Vertx

    @Mock
    private lateinit var mongoClient: MongoClient

    @Mock
    private lateinit var entityFactory: EntityFactory

    private lateinit var entityReflector: EntityReflector
    private lateinit var mongoEntityStore: MongoEntityStore
    private val entityCollection = "entities"

    @BeforeEach
    fun setup(testContext: VertxTestContext) {
        vertx = Vertx.vertx()
        MockitoAnnotations.openMocks(this)

        entityReflector = EntityReflector()
        entityReflector.registerEntityClasses(
            listOf(
                Region::class,
                Zone::class,
                Building::class,
                MapUnit::class,
                MapVector2::class
            )
        )

        mongoEntityStore = MongoEntityStore(
            mongoClient,
            entityCollection,
            entityFactory,
            entityReflector
        )

        // Configure any needed mock behavior here

        testContext.completeNow()
    }

    private fun createTestFixture(): Region {
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
    private fun createTestJson(): JsonArray {
        val documents = JsonArray()

        // Root Region
        documents.add(JsonObject()
            .put("_id", "507f1f77bcf86cd799439011")
            .put("type", "Region")
            .put("version", 1)
            .put("state", JsonObject()
                .put("name", "Test Region Alpha")
                .put("zones", JsonArray()
                    .add(JsonObject()
                        .put("\$ref", "entity")
                        .put("\$id", "507f1f77bcf86cd799439012"))
                    .add(JsonObject()
                        .put("\$ref", "entity")
                        .put("\$id", "507f1f77bcf86cd799439013")))))

        // Safe Zone
        documents.add(JsonObject()
            .put("_id", "507f1f77bcf86cd799439013")
            .put("type", "Zone")
            .put("version", 1)
            .put("state", JsonObject()
                .put("buildings", JsonArray())
                .put("name", "Safe Zone")
                .put("region", JsonObject()
                    .put("\$ref", "entity")
                    .put("\$id", "507f1f77bcf86cd799439011"))
                .put("units", JsonArray()
                    .add(JsonObject()
                        .put("\$ref", "entity")
                        .put("\$id", "507f1f77bcf86cd799439017")))))

        // Field Medic
        documents.add(JsonObject()
            .put("_id", "507f1f77bcf86cd799439017")
            .put("type", "MapUnit")
            .put("version", 1)
            .put("state", JsonObject()
                .put("name", "Field Medic")
                .put("offense", JsonObject()
                    .put("burn", false)
                    .put("AP", false)
                    .put("melee", false)
                    .put("damage", JsonArray().add(1)))
                .put("position", JsonObject()
                    .put("\$ref", "entity")
                    .put("\$id", "507f1f77bcf86cd799439021"))
                .put("statuses", JsonArray().add("healing"))
                .put("zone", JsonObject()
                    .put("\$ref", "entity")
                    .put("\$id", "507f1f77bcf86cd799439013"))))

        // Medic Position Vector
        documents.add(JsonObject()
            .put("_id", "507f1f77bcf86cd799439021")
            .put("type", "MapVector2")
            .put("version", 1)
            .put("state", JsonObject()
                .put("parentEntity", JsonObject()
                    .put("\$ref", "entity")
                    .put("\$id", "507f1f77bcf86cd799439017"))
                .put("x", 200.0)
                .put("y", 200.0)))

        // Combat Zone
        documents.add(JsonObject()
            .put("_id", "507f1f77bcf86cd799439012")
            .put("type", "Zone")
            .put("version", 1)
            .put("state", JsonObject()
                .put("buildings", JsonArray()
                    .add(JsonObject()
                        .put("\$ref", "entity")
                        .put("\$id", "507f1f77bcf86cd799439014"))
                    .add(JsonObject()
                        .put("\$ref", "entity")
                        .put("\$id", "507f1f77bcf86cd799439015")))
                .put("name", "Combat Zone")
                .put("region", JsonObject()
                    .put("\$ref", "entity")
                    .put("\$id", "507f1f77bcf86cd799439011"))
                .put("units", JsonArray()
                    .add(JsonObject()
                        .put("\$ref", "entity")
                        .put("\$id", "507f1f77bcf86cd799439016")))))

        // Barracks
        documents.add(JsonObject()
            .put("_id", "507f1f77bcf86cd799439015")
            .put("type", "Building")
            .put("version", 1)
            .put("state", JsonObject()
                .put("name", "Barracks")
                .put("position", JsonObject()
                    .put("\$ref", "entity")
                    .put("\$id", "507f1f77bcf86cd799439019"))
                .put("statuses", JsonArray())
                .put("zone", JsonObject()
                    .put("\$ref", "entity")
                    .put("\$id", "507f1f77bcf86cd799439012"))))

        // Headquarters
        documents.add(JsonObject()
            .put("_id", "507f1f77bcf86cd799439014")
            .put("type", "Building")
            .put("version", 1)
            .put("state", JsonObject()
                .put("name", "Headquarters")
                .put("position", JsonObject()
                    .put("\$ref", "entity")
                    .put("\$id", "507f1f77bcf86cd799439018"))
                .put("statuses", JsonArray()
                    .add("powered")
                    .add("fortified"))
                .put("zone", JsonObject()
                    .put("\$ref", "entity")
                    .put("\$id", "507f1f77bcf86cd799439012"))))

        // Elite Soldier
        documents.add(JsonObject()
            .put("_id", "507f1f77bcf86cd799439016")
            .put("type", "MapUnit")
            .put("version", 1)
            .put("state", JsonObject()
                .put("name", "Elite Soldier")
                .put("offense", JsonObject()
                    .put("burn", true)
                    .put("AP", false)
                    .put("melee", true)
                    .put("damage", JsonArray().add(5).add(10).add(15)))
                .put("position", JsonObject()
                    .put("\$ref", "entity")
                    .put("\$id", "507f1f77bcf86cd799439020"))
                .put("statuses", JsonArray()
                    .add("alert")
                    .add("armed"))
                .put("zone", JsonObject()
                    .put("\$ref", "entity")
                    .put("\$id", "507f1f77bcf86cd799439012"))))

        // Soldier Position Vector
        documents.add(JsonObject()
            .put("_id", "507f1f77bcf86cd799439020")
            .put("type", "MapVector2")
            .put("version", 1)
            .put("state", JsonObject()
                .put("parentEntity", JsonObject()
                    .put("\$ref", "entity")
                    .put("\$id", "507f1f77bcf86cd799439016"))
                .put("x", 120.0)
                .put("y", 120.0)))

        return documents
    }

    /**@Test
    fun testSerialize_success(testContext: VertxTestContext) {
    val region = createTestFixture()
    val expected = createTestJson()

    // Keep hash printing for determinism check
    val expectedHash = hashJsonObject(expected)

    // Option 2: Using coroutines (new approach)
    // Uncomment this when you fully migrate to coroutines
    testContext.verify {
    runBlocking {
    val serialized = region.serialize()
    val actualHash = hashJsonObject(serialized)

    // Print hashes for debugging
    println("Expected hash: $expectedHash")
    println("Actual hash: $actualHash")

    // AssertJ deep comparison
    assertThat(serialized)
    .usingRecursiveComparison()
    .isEqualTo(expected)

    // Also verify hash equality for determinism
    assertThat(actualHash).isEqualTo(expectedHash)

    testContext.completeNow()
    }
    }
    }**/

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

    @Test
    fun testBuildAncestorTraversalQuery_success(testContext: VertxTestContext) = runBlocking {
        val entityId = "507f1f77bcf86cd799439020" // MapVector2 ID
        val parentRefPaths = listOf("state.parentEntity")
        val expectedQuery = createExpectedAncestorTraversalQuery(entityId, "entities", parentRefPaths)

        // When
        val actualQuery = mongoEntityStore.buildAncestorTraversalQuery(entityId)

        // Then
        assertThat(actualQuery)
            .usingRecursiveComparison()
            .isEqualTo(expectedQuery)

        testContext.completeNow()
    }

    fun createMockAggregationResults(): JsonArray {
        // This represents the results of a MongoDB $graphLookup aggregation
        // starting from the soldier's position vector and finding all ancestors
        return JsonArray().add(
            JsonObject()
                .put("_id", "507f1f77bcf86cd799439020") // MapVector2 (soldier's position)
                .put("type", "MapVector2")
                .put("version", 1)
                .put("state", JsonObject()
                    .put("parentEntity", JsonObject()
                        .put("\$ref", "entity")
                        .put("\$id", "507f1f77bcf86cd799439016")) // Refers to soldier
                    .put("x", 120.0)
                    .put("y", 120.0))
                .put("ancestors", JsonArray()
                    .add(JsonObject() // First ancestor: the soldier
                        .put("_id", "507f1f77bcf86cd799439016")
                        .put("type", "MapUnit")
                        .put("version", 1)
                        .put("depth", 0)
                        .put("state", JsonObject()
                            .put("name", "Elite Soldier")
                            .put("zone", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439012")) // Refers to combat zone
                            .put("position", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439020")) // Refers back to vector
                            .put("statuses", JsonArray().add("alert").add("armed"))
                            .put("offense", JsonObject()
                                .put("burn", true)
                                .put("AP", false)
                                .put("melee", true)
                                .put("damage", JsonArray().add(5).add(10).add(15)))))
                    .add(JsonObject() // Second ancestor: combat zone
                        .put("_id", "507f1f77bcf86cd799439012")
                        .put("type", "Zone")
                        .put("version", 1)
                        .put("depth", 1)
                        .put("state", JsonObject()
                            .put("name", "Combat Zone")
                            .put("region", JsonObject()
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439011")) // Refers to region
                            .put("units", JsonArray()
                                .add(JsonObject()
                                    .put("\$ref", "entity")
                                    .put("\$id", "507f1f77bcf86cd799439016"))) // Refers to soldier
                            .put("buildings", JsonArray()
                                .add(JsonObject()
                                    .put("\$ref", "entity")
                                    .put("\$id", "507f1f77bcf86cd799439014")) // HQ
                                .add(JsonObject()
                                    .put("\$ref", "entity")
                                    .put("\$id", "507f1f77bcf86cd799439015"))))) // Barracks
                    .add(JsonObject() // Third ancestor: region
                        .put("_id", "507f1f77bcf86cd799439011")
                        .put("type", "Region")
                        .put("version", 1)
                        .put("depth", 2)
                        .put("state", JsonObject()
                            .put("name", "Test Region Alpha")
                            .put("zones", JsonArray()
                                .add(JsonObject()
                                    .put("\$ref", "entity")
                                    .put("\$id", "507f1f77bcf86cd799439012")) // Combat Zone
                                .add(JsonObject()
                                    .put("\$ref", "entity")
                                    .put("\$id", "507f1f77bcf86cd799439013"))))) // Safe Zone
                    // Add buildings that reference zones but aren't in the parent reference chain
                    .add(JsonObject() // Building: HQ
                        .put("_id", "507f1f77bcf86cd799439014")
                        .put("type", "Building")
                        .put("version", 1)
                        .put("depth", 1) // Same depth as zone to make it seem like a sibling
                        .put("state", JsonObject()
                            .put("name", "Headquarters")
                            .put("zone", JsonObject() // References combat zone but not as a parent reference
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439012"))
                            .put("statuses", JsonArray()
                                .add("powered")
                                .add("fortified"))))
                    .add(JsonObject() // Building: Barracks
                        .put("_id", "507f1f77bcf86cd799439015")
                        .put("type", "Building")
                        .put("version", 1)
                        .put("depth", 1)
                        .put("state", JsonObject()
                            .put("name", "Barracks")
                            .put("zone", JsonObject() // References combat zone but not as a parent reference
                                .put("\$ref", "entity")
                                .put("\$id", "507f1f77bcf86cd799439012"))
                            .put("statuses", JsonArray())))
                )
        )
    }

    @Test
    fun testTransformResultsToEntityGraph(testContext: VertxTestContext) {
        // Given
        val jsonArrayResults = createMockAggregationResults()

        // Configure mocks to return appropriate entities
        Mockito.`when`(entityFactory.getNew("MapVector2")).thenAnswer { MapVector2() }
        Mockito.`when`(entityFactory.getNew("MapUnit")).thenAnswer { MapUnit() }
        Mockito.`when`(entityFactory.getNew("Zone")).thenAnswer { Zone() }
        Mockito.`when`(entityFactory.getNew("Region")).thenAnswer { Region() }
        Mockito.`when`(entityFactory.getNew("Building")).thenAnswer { Building() }

        // When
        val graph = mongoEntityStore.transformResultsToEntityGraph(jsonArrayResults)

        // Then
        testContext.verify {
            // Verify vertex count - we should have 4 entities in our graph
            // (MapVector2, MapUnit, Zone, Region)
            assertEquals(6, graph.vertexSet().size, "Graph should have 6 vertices")

            // DEBUG
            val allEdges = graph.edgeSet()

            // Find our entities in the graph
            val vector = graph.vertexSet().find { it._id == "507f1f77bcf86cd799439020" }
            val soldier = graph.vertexSet().find { it._id == "507f1f77bcf86cd799439016" }
            val zone = graph.vertexSet().find { it._id == "507f1f77bcf86cd799439012" }
            val region = graph.vertexSet().find { it._id == "507f1f77bcf86cd799439011" }
            val building = graph.vertexSet().find { it._id == "507f1f77bcf86cd799439015" }
            val building2 = graph.vertexSet().find { it._id == "507f1f77bcf86cd799439014" }

            // Verify all entities were found
            assertNotNull(vector, "Position vector should be in the graph")
            assertNotNull(soldier, "Soldier should be in the graph")
            assertNotNull(zone, "Combat zone should be in the graph")
            assertNotNull(region, "Region should be in the graph")
            assertNotNull(building, "Barracks building should be in the graph")
            assertNotNull(building2, "Headquarters building should be in the graph")

            // Verify edge from vector to soldier (vector's owner is soldier)
            assertTrue(graph.containsEdge(vector, soldier),
                "There should be an edge from position vector to soldier")

            // Verify edge from soldier to zone
            assertTrue(graph.containsEdge(soldier, zone),
                "There should be an edge from soldier to zone")

            // Verify edge from zone to region
            assertTrue(graph.containsEdge(zone, region),
                "There should be an edge from zone to region")

            // Verify edge from zone to region
            assertTrue(graph.containsEdge(building2, zone),
                "There should be an edge from headquarters to zone")

            assertTrue(graph.containsEdge(building, zone),
                "There should be an edge from barracks to zone")

            // Complete the test
            testContext.completeNow()
        }
    }

    private fun hashJsonObject(json: JsonArray): String {
        try {
            val digest = MessageDigest.getInstance("SHA-256")
            val hash = digest.digest(json.encode().toByteArray())
            val hexString = StringBuilder()
            for (b in hash) {
                val hex = Integer.toHexString(0xff and b.toInt())
                if (hex.length == 1) hexString.append('0')
                hexString.append(hex)
            }
            return hexString.toString()
        } catch (e: Exception) {
            throw RuntimeException(e)
        }
    }

    @Test
    fun testFindEntitiesForVersionUpdate(testContext: VertxTestContext) {
        // Given
        // Create a simple graph structure with entities and their relationships
        val graph = DefaultDirectedGraph<Entity, DefaultEdge>(DefaultEdge::class.java)

        // First, set up a real EntityReflector with the actual entity classes
        val realReflector = EntityReflector()

        // Create test entities
        val vector = MapVector2().apply {
            _id = "507f1f77bcf86cd799439020"
            entityReflector = realReflector
        }

        val soldier = MapUnit().apply {
            _id = "507f1f77bcf86cd799439016"
            entityReflector = realReflector
        }

        val zone = Zone().apply {
            _id = "507f1f77bcf86cd799439012"
            entityReflector = realReflector
        }

        val region = Region().apply {
            _id = "507f1f77bcf86cd799439011"
            entityReflector = realReflector
        }

        val building = Building().apply {
            _id = "507f1f77bcf86cd799439015"
            entityReflector = realReflector
        }

        val building2 = Building().apply {
            _id = "507f1f77bcf86cd799439014"
            entityReflector = realReflector
        }

        // Initialize the reflection cache with actual entity classes
        // This will populate the cache with real reflection data based on annotations
        realReflector.buildReflectionCache(MapVector2::class.java)
        realReflector.buildReflectionCache(MapUnit::class.java)
        realReflector.buildReflectionCache(Zone::class.java)
        realReflector.buildReflectionCache(Region::class.java)
        realReflector.buildReflectionCache(Building::class.java)

        // Add entities to the graph
        graph.addVertex(vector)
        graph.addVertex(soldier)
        graph.addVertex(zone)
        graph.addVertex(region)
        graph.addVertex(building)
        graph.addVertex(building2)

        // Add relationships (edges)
        graph.addEdge(vector, soldier) // Vector points to soldier (parentEntity)
        graph.addEdge(soldier, zone)   // Soldier points to zone
        graph.addEdge(zone, region)    // Zone points to region
        graph.addEdge(building, zone)  // Building points to zone (but not as parent reference)
        graph.addEdge(building2, zone) // Building2 points to zone (but not as parent reference)

        // When
        val idsToUpdate = vector.findEntitiesForVersionUpdate(graph)

        // Then
        testContext.verify {
            // Print debug info to help understand how the real reflection cache is configured
            println("MapVector2 parent refs: ${realReflector.getReflectionCache(MapVector2::class.java)?.parentReferenceFields}")
            println("MapUnit parent refs: ${realReflector.getReflectionCache(MapUnit::class.java)?.parentReferenceFields}")
            println("Zone parent refs: ${realReflector.getReflectionCache(Zone::class.java)?.parentReferenceFields}")
            println("Building parent refs: ${realReflector.getReflectionCache(Building::class.java)?.parentReferenceFields}")

            // We expect only entities in the parent reference chain to be updated
            assertEquals(4, idsToUpdate.size, "Only 4 entities should need updates")

            // Verify each ID is in the set
            assertTrue(idsToUpdate.contains("507f1f77bcf86cd799439020"), "Vector ID should be in update set")
            assertTrue(idsToUpdate.contains("507f1f77bcf86cd799439016"), "Soldier ID should be in update set")
            assertTrue(idsToUpdate.contains("507f1f77bcf86cd799439012"), "Zone ID should be in update set")
            assertTrue(idsToUpdate.contains("507f1f77bcf86cd799439011"), "Region ID should be in update set")

            // Building should not be included as they don't use @ParentReference for zone
            assertFalse(idsToUpdate.contains("507f1f77bcf86cd799439015"), "Headquarters should not be in update set")
            assertFalse(idsToUpdate.contains("507f1f77bcf86cd799439014"), "Barracks should not be in update set")

            // Complete the test
            testContext.completeNow()
        }
    }
}

@JsonTypeName("Region")
@EntityType("Region")
class Region : Entity() {
    @State
    @JsonProperty("zones")
    var zones: ArrayList<Zone> = ArrayList()

    @State
    @JsonProperty("name")
    var name: String = ""
}

@JsonTypeName("Zone")
@EntityType("Zone")
class Zone : Entity() {
    @JsonProperty("name")
    @State
    var name: String = ""

    @State
    @JsonProperty("region")
    @ParentReference
    lateinit var region: Region  // back-reference

    @JsonProperty("units")
    @State
    @MutateStrict
    @ChildReference
    var units: ArrayList<MapUnit> = ArrayList()

    @JsonProperty("buildings")
    @State
    @ChildReference
    var buildings: ArrayList<Building> = ArrayList()
}

@JsonTypeName("Building")
@EntityType("Building")
class Building : Entity() {
    @JsonProperty("name")
    @State
    var name: String = ""

    @JsonProperty("zone")
    @State
    @ParentReference
    lateinit var zone: Zone  // back-reference

    @JsonProperty("position")
    @State
    @ChildReference
    var position: MapVector2? = null

    @JsonProperty("statuses")
    @State
    var statuses: HashSet<String> = HashSet()
}

@JsonTypeName("MapUnit")
@EntityType("MapUnit")
class MapUnit : Entity() {
    @JsonProperty("name")
    @State
    var name: String = ""

    @JsonProperty("zone")
    @State
    @ParentReference
    lateinit var zone: Zone  // back-reference

    @JsonProperty("position")
    @State
    @ChildReference
    lateinit var position: MapVector2

    @JsonProperty("statuses")
    @State
    var statuses: HashSet<String> = HashSet()

    @JsonProperty("offense")
    @State
    lateinit var offense: JsonObject  // contains burn, AP, melee booleans and int array
}

@JsonTypeName("MapVector2")
@EntityType("MapVector2")
class MapVector2 : Entity() {
    @JsonProperty("x")
    @State
    var x: Double = 0.0

    @JsonProperty("y")
    @State
    var y: Double = 0.0

    @JsonProperty("parentEntity")
    @State
    @ParentReference
    lateinit var parentEntity: MapUnit  // back-reference to either Building or MapUnit
}

class TestEntityFactory : EntityFactory {
    private val classes: HashMap<String, Class<*>> = HashMap()

    init {
        // Register our base types
        classes.put("region", Region::class.java)
        classes.put("zone", Zone::class.java)
        classes.put("building", Building::class.java)
        classes.put("mapunit", MapUnit::class.java)
        classes.put("mapvector2", MapVector2::class.java)
    }

    override fun useClass(type: String): Class<*>? {
        return classes[type.lowercase()]
    }

    override fun getNew(type: String): Entity {
        return when (type.lowercase()) {
            "region" -> Region()
            "zone" -> Zone()
            "building" -> Building()
            "mapunit" -> MapUnit()
            "mapvector2" -> MapVector2()
            else -> Entity()
        }
    }
}