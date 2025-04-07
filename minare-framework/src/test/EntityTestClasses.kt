import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonTypeName
import com.minare.core.entity.EntityFactory
import com.minare.core.entity.annotations.*
import com.minare.core.models.Entity
import io.vertx.core.json.JsonObject
import kotlin.reflect.KClass

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
    @Parent
    lateinit var region: Region

    @JsonProperty("units")
    @State
    @Mutable
    @Child
    var units: ArrayList<MapUnit> = ArrayList()

    @JsonProperty("buildings")
    @State
    @Child
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
    @Parent
    lateinit var zone: Zone

    @JsonProperty("position")
    @State
    @Child
    lateinit var position: MapVector2

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
    @Parent
    lateinit var zone: Zone

    @JsonProperty("position")
    @State
    @Child
    lateinit var position: MapVector2

    @JsonProperty("statuses")
    @State
    @Mutable(ConsistencyLevel.PESSIMISTIC)
    var statuses: HashSet<String> = HashSet()

    @JsonProperty("offense")
    @State
    lateinit var offense: JsonObject
}

@JsonTypeName("MapVector2")
@EntityType("MapVector2")
class MapVector2 : Entity() {
    @JsonProperty("x")
    @State
    @Mutable(ConsistencyLevel.STRICT)
    var x: Double = 0.0

    @JsonProperty("y")
    @State
    @Mutable(ConsistencyLevel.STRICT)
    var y: Double = 0.0

    @JsonProperty("parentEntity")
    @State
    @Parent
    lateinit var parentEntity: Entity
}

class TestEntityFactory : EntityFactory {
    private val classes: HashMap<String, Class<*>> = HashMap()

    init {

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

    @Suppress("UNCHECKED_CAST")
    override fun <T : Entity> createEntity(entityClass: Class<T>): T {
        val entity = when {
            Region::class.java.isAssignableFrom(entityClass) -> Region() as T
            Zone::class.java.isAssignableFrom(entityClass) -> Zone() as T
            Building::class.java.isAssignableFrom(entityClass) -> Building() as T
            MapUnit::class.java.isAssignableFrom(entityClass) -> MapUnit() as T
            MapVector2::class.java.isAssignableFrom(entityClass) -> MapVector2() as T
            Entity::class.java.isAssignableFrom(entityClass) -> Entity() as T
            else -> {
                Entity() as T
            }
        }


        return ensureDependencies(entity)
    }

    /**
     * Helper method to get registered entity types - implement based on your EntityFactory
     */
    override fun getTypeNames(): List<String> {
        return listOf("MapVector2", "MapUnit", "Zone", "Region", "Building")
    }

    override fun getTypeList(): List<KClass<*>> {
        return listOf(
            Region::class,
            Zone::class,
            Building::class,
            MapUnit::class,
            MapVector2::class
        )
    }

    /**
     * Ensure an entity has all required dependencies injected
     */
    override fun <T : Entity> ensureDependencies(entity: T): T {

        return entity
    }
}