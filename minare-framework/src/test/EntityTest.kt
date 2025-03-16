import com.minare.core.models.Entity
import com.minare.core.entity.EntityFactory
import com.minare.core.entity.EntityReflector
import com.minare.persistence.MongoEntityStore
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
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

    val testFixtures: EntityTestFixtures = EntityTestFixtures()

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

    @Test
    fun testBuildAncestorTraversalQuery_success(testContext: VertxTestContext) = runBlocking {
        val entityId = "507f1f77bcf86cd799439020" // MapVector2 ID
        val parentRefPaths = listOf("state.parentEntity")
        val expectedQuery = testFixtures.createExpectedAncestorTraversalQuery(entityId, "entities", parentRefPaths)

        // When
        val actualQuery = mongoEntityStore.buildAncestorTraversalQuery(entityId)

        // Then
        assertThat(actualQuery)
            .usingRecursiveComparison()
            .isEqualTo(expectedQuery)

        testContext.completeNow()
    }

    @Test
    fun testTransformResultsToEntityGraph(testContext: VertxTestContext) {
        // Given
        val jsonArrayResults = testFixtures.createMockAggregationResults()

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

