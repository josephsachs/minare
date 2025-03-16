import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.annotation.JsonTypeName
import com.minare.core.entity.EntityFactory
import com.minare.core.entity.annotations.*
import com.minare.core.models.Entity
import io.vertx.core.json.JsonObject

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
    lateinit var region: Region  // back-reference

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
    lateinit var zone: Zone  // back-reference

    @JsonProperty("position")
    @State
    @Child
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
    @Parent
    lateinit var zone: Zone  // back-reference

    @JsonProperty("position")
    @State
    @Child
    lateinit var position: MapVector2

    @JsonProperty("statuses")
    @State
    @Mutable
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
    @Mutable
    var x: Double = 0.0

    @JsonProperty("y")
    @State
    @Mutable
    var y: Double = 0.0

    @JsonProperty("parentEntity")
    @State
    @Parent
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