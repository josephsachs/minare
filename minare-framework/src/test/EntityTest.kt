/** As of Redis PubSub refactor, the Entity interface has changed completely
 These tests helped us develop the graphing stuff. They served their purpose. And now, like adult mayflies, they die.
 Another reminder that until you know what your unit SUTs are, your tests are ephemeral as hell. **/


/**import com.google.inject.AbstractModule
import com.google.inject.Guice
import com.google.inject.Injector
import com.google.inject.name.Names
import kotlin.com.minare.core.entity.models.Entity
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Parent
import com.minare.persistence.EntityStore
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
    private lateinit var testInjector: Injector

    @Mock
    private lateinit var mongoClient: MongoClient

    @Mock
    private lateinit var entityFactory: EntityFactory

    private lateinit var reflectionCache: ReflectionCache
    private lateinit var mongoEntityStore: MongoEntityStore
    private val entityCollection = "entities"

    val testFixtures: EntityTestFixtures = EntityTestFixtures()

    @BeforeEach
    fun setup(testContext: VertxTestContext) {
        vertx = Vertx.vertx()
        MockitoAnnotations.openMocks(this)


        reflectionCache = ReflectionCache()


        setupEntityFactoryMocks()


        reflectionCache.registerEntityClasses(
            listOf(
                Region::class,
                Zone::class,
                Building::class,
                MapUnit::class,
                MapVector2::class
            )
        )


        testInjector = Guice.createInjector(object : AbstractModule() {
            override fun configure() {
                bind(ReflectionCache::class.java).toInstance(reflectionCache)
                bind(MongoClient::class.java).toInstance(mongoClient)
                bind(EntityFactory::class.java).toInstance(entityFactory)


                bind(EntityStore::class.java).to(MongoEntityStore::class.java)


                bind(String::class.java)
                    .annotatedWith(Names.named("entityCollection"))
                    .toInstance(entityCollection)
            }
        })


        mongoEntityStore = testInjector.getInstance(MongoEntityStore::class.java)

        testContext.completeNow()
    }

    /**
     * Helper method to set up entity factory behavior
     */
    private fun setupEntityFactoryMocks() {

        Mockito.`when`(entityFactory.useClass("MapVector2")).thenReturn(MapVector2::class.java)
        Mockito.`when`(entityFactory.useClass("MapUnit")).thenReturn(MapUnit::class.java)
        Mockito.`when`(entityFactory.useClass("Zone")).thenReturn(Zone::class.java)
        Mockito.`when`(entityFactory.useClass("Region")).thenReturn(Region::class.java)
        Mockito.`when`(entityFactory.useClass("Building")).thenReturn(Building::class.java)
    }

    /**
     * Helper method to inject dependencies into entity instances
     */
    @Test
    fun testSerialize_success(testContext: VertxTestContext) {

        val region = testFixtures.createTestFixture(testInjector)

        val expected = testFixtures.createTestJson()
        val expectedHash = hashJsonObject(expected)

        testContext.verify {
            runBlocking {
                val serialized = region.serialize()
                val actualHash = hashJsonObject(serialized)


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
        Mockito.`when`(entityFactory.useClass("MapVector2")).thenReturn(MapVector2::class.java)
        Mockito.`when`(entityFactory.useClass("MapUnit")).thenReturn(MapUnit::class.java)
        Mockito.`when`(entityFactory.useClass("Zone")).thenReturn(Zone::class.java)
        Mockito.`when`(entityFactory.useClass("Region")).thenReturn(Region::class.java)
        Mockito.`when`(entityFactory.useClass("Building")).thenReturn(Building::class.java)

        // Set up entityFactory.getNew to return properly initialized entities
        Mockito.`when`(entityFactory.getNew("MapVector2")).thenAnswer {
            val entity = MapVector2()
            testInjector.injectMembers(entity)
            entity
        }

        Mockito.`when`(entityFactory.getNew("MapUnit")).thenAnswer {
            val entity = MapUnit()
            testInjector.injectMembers(entity)
            entity
        }

        Mockito.`when`(entityFactory.getNew("Zone")).thenAnswer {
            val entity = Zone()
            testInjector.injectMembers(entity)
            entity
        }

        Mockito.`when`(entityFactory.getNew("Region")).thenAnswer {
            val entity = Region()
            testInjector.injectMembers(entity)
            entity
        }

        Mockito.`when`(entityFactory.getNew("Building")).thenAnswer {
            val entity = Building()
            testInjector.injectMembers(entity)
            entity
        }

        try {
            // When
            val graph = mongoEntityStore.transformResultsToEntityGraph(jsonArrayResults)

            // Then
            testContext.verify {
                // Debug: print entity count
                println("Graph vertices count: ${graph.vertexSet().size}")

                // Verify vertex count
                assertEquals(6, graph.vertexSet().size, "Graph should have 6 vertices")

                // Rest of the test...

                testContext.completeNow()
            }
        } catch (e: Exception) {
            e.printStackTrace()
            testContext.failNow(e)
        }
    }

    @Test
    fun testFindEntitiesForVersionUpdate(testContext: VertxTestContext) {
        // Given
        // Create a simple graph structure with entities and their relationships
        val graph = DefaultDirectedGraph<Entity, DefaultEdge>(DefaultEdge::class.java)

        // Create test entities with proper dependency injection
        val vector = createTestEntity(MapVector2::class.java, "507f1f77bcf86cd799439020", "MapVector2") {}

        val soldier = createTestEntity(MapUnit::class.java, "507f1f77bcf86cd799439016", "MapUnit") {}

        val zone = createTestEntity(Zone::class.java, "507f1f77bcf86cd799439012", "Zone") {}

        val region = createTestEntity(Region::class.java, "507f1f77bcf86cd799439011", "Region") {}

        val building = createTestEntity(Building::class.java, "507f1f77bcf86cd799439015", "Building") {}

        val building2 = createTestEntity(Building::class.java, "507f1f77bcf86cd799439014", "Building") {}

        // IMPORTANT: Set up the actual parent references in the entity objects
        // These need to be set since shouldBubbleVersionToParent() checks these fields,
        // not just the graph structure
        vector.parentEntity = soldier
        soldier.zone = zone
        zone.region = region
        building.zone = zone
        building2.zone = zone

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
            println("MapVector2 parent fields: ${reflectionCache.getFieldsWithAnnotation<Parent>(MapVector2::class).map { it.name }}")
            println("MapUnit parent fields: ${reflectionCache.getFieldsWithAnnotation<Parent>(MapUnit::class).map { it.name }}")
            println("Zone parent fields: ${reflectionCache.getFieldsWithAnnotation<Parent>(Zone::class).map { it.name }}")
            println("Building parent fields: ${reflectionCache.getFieldsWithAnnotation<Parent>(Building::class).map { it.name }}")

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

    @Test
    fun testMutatePruning(testContext: VertxTestContext) {
        // Arrange - create the entity using Guice
        val mapVector = testInjector.getInstance(MapVector2::class.java)
        mapVector._id = "test_vector_id"

        // Get test cases
        val testCases = testFixtures.createMapVector2MutationTestCases()

        testContext.verify {
            // Act & Assert - test each case
            for ((index, testCase) in testCases.withIndex()) {
                runBlocking {
                    val prunedDelta = mapVector.getMutateDelta(testCase.inputDelta)

                    // Assert pruned correctly
                    assertThat(prunedDelta)
                        .usingRecursiveComparison()
                        .isEqualTo(testCase.expectedPrunedDelta)

                    println("Test case $index passed!")
                }
            }

            testContext.completeNow()
        }
    }

    @Test
    fun testFilterDeltaByConsistencyLevel(testContext: VertxTestContext) {
        // Create test entities using Guice injection
        val mapVector = testInjector.getInstance(MapVector2::class.java).apply {
            _id = "test_vector_id"
            version = 5 // Set a specific version for testing
        }

        val mapUnit = testInjector.getInstance(MapUnit::class.java).apply {
            _id = "test_unit_id"
            version = 5 // Match version with mapVector for consistency
        }

        // Define test cases
        val testCases = testFixtures.createConsistencyLevelTestCases()

        testContext.verify {
            // Act & Assert - test each case
            for ((index, testCase) in testCases.withIndex()) {
                runBlocking {
                    // Get the right entity based on test case type
                    val entity = when (testCase.entityType) {
                        "MapVector2" -> mapVector
                        "MapUnit" -> mapUnit
                        else -> throw IllegalArgumentException("Unknown entity type")
                    }

                    // Execute the filter function
                    val filteredDelta = entity.filterDeltaByConsistencyLevel(
                        testCase.inputDelta,
                        testCase.requestedVersion
                    )

                    // Assert filtered correctly
                    assertThat(filteredDelta)
                        .usingRecursiveComparison()
                        .isEqualTo(testCase.expectedFilteredDelta)

                    println("Consistency level test case $index passed!")
                }
            }

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

    private fun <T : Entity> createTestEntity(entityClass: Class<T>, id: String, type: String, configure: (T) -> Unit): T {
        val entity = entityClass.newInstance()
        entity._id = id
        entity.type = type
        configure(entity)
        testInjector.injectMembers(entity)
        return entity
    }
}**/