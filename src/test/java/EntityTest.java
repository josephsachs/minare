import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.minare.core.models.annotations.entity.*;
import com.minare.core.models.Entity;
import com.minare.core.entity.EntityFactory;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import io.vertx.ext.mongo.MongoClient;

import java.security.MessageDigest;
import java.util.*;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(VertxExtension.class)
public class EntityTest {
    private Vertx vertx;

    @Mock
    private MongoClient mongoClient;

    @BeforeEach
    void setup(VertxTestContext testContext) {
        vertx = Vertx.vertx();
        MockitoAnnotations.openMocks(this);
        // Configure any needed mock behavior here
        testContext.completeNow();
    }

    private Region createTestFixture() {
        Region region = new Region();
        region._id = "507f1f77bcf86cd799439011";
        region.name = "Test Region Alpha";
        region.zones = new ArrayList<>();

        Zone zone1 = new Zone();
        zone1._id = "507f1f77bcf86cd799439012";
        zone1.name = "Combat Zone";
        zone1.region = region;
        zone1.units = new ArrayList<>();
        zone1.buildings = new ArrayList<>();

        Zone zone2 = new Zone();
        zone2._id = "507f1f77bcf86cd799439013";
        zone2.name = "Safe Zone";
        zone2.region = region;
        zone2.units = new ArrayList<>();
        zone2.buildings = new ArrayList<>();

        Building hq = new Building();
        hq._id = "507f1f77bcf86cd799439014";
        hq.name = "Headquarters";
        hq.zone = zone1;
        //hq.position = new MapVector2();
        //hq.position._id = "507f1f77bcf86cd799439018";
        //hq.position.x = 100;
        //hq.position.y = 100;
        //hq.position.owner = hq;
        hq.statuses = new HashSet<>(Arrays.asList("fortified", "powered"));

        Building barracks = new Building();
        barracks._id = "507f1f77bcf86cd799439015";
        barracks.name = "Barracks";
        barracks.zone = zone1;
        //barracks.position = new MapVector2();
        //barracks.position._id = "507f1f77bcf86cd799439019";
        //barracks.position.x = 150;
        //barracks.position.y = 100;
        //barracks.position.owner = barracks;
        barracks.statuses = new HashSet<>();  // empty set

        MapUnit soldier = new MapUnit();
        soldier._id = "507f1f77bcf86cd799439016";
        soldier.name = "Elite Soldier";
        soldier.zone = zone1;
        soldier.position = new MapVector2();
        soldier.position._id = "507f1f77bcf86cd799439020";
        soldier.position.x = 120;
        soldier.position.y = 120;
        soldier.position.owner = soldier;
        soldier.statuses = new HashSet<>(Arrays.asList("alert", "armed"));
        soldier.offense = new JsonObject()
                .put("burn", true)
                .put("AP", false)
                .put("melee", true)
                .put("damage", new JsonArray().add(5).add(10).add(15));

        MapUnit medic = new MapUnit();
        medic._id = "507f1f77bcf86cd799439017";
        medic.name = "Field Medic";
        medic.zone = zone2;  // in safe zone
        medic.position = new MapVector2();
        medic.position._id = "507f1f77bcf86cd799439021";
        medic.position.x = 200;
        medic.position.y = 200;
        medic.position.owner = medic;
        medic.statuses = new HashSet<>(Arrays.asList("healing"));
        medic.offense = new JsonObject()
                .put("burn", false)
                .put("AP", false)
                .put("melee", false)
                .put("damage", new JsonArray().add(1));

        // Wire up all the references
        zone1.buildings.addAll(Arrays.asList(hq, barracks));
        zone1.units.add(soldier);
        zone2.units.add(medic);
        region.zones.addAll(Arrays.asList(zone1, zone2));

        return region;
    }

    // The expected serialized form (would be generated from actual output)
    private JsonArray createTestJson() {
        JsonArray documents = new JsonArray();

        // Root Region
        documents.add(new JsonObject()
                .put("_id", "507f1f77bcf86cd799439011")
                .put("type", "Region")
                .put("version", 1)
                .put("state", new JsonObject()
                        .put("name", "Test Region Alpha")
                        .put("zones", new JsonArray()
                                .add(new JsonObject()
                                        .put("$ref", "entity")
                                        .put("$id", "507f1f77bcf86cd799439012"))
                                .add(new JsonObject()
                                        .put("$ref", "entity")
                                        .put("$id", "507f1f77bcf86cd799439013")))));

        // Safe Zone
        documents.add(new JsonObject()
                .put("_id", "507f1f77bcf86cd799439013")
                .put("type", "Zone")
                .put("version", 1)
                .put("state", new JsonObject()
                        .put("buildings", new JsonArray())
                        .put("name", "Safe Zone")
                        .put("region", new JsonObject()
                                .put("$ref", "entity")
                                .put("$id", "507f1f77bcf86cd799439011"))
                        .put("units", new JsonArray()
                                .add(new JsonObject()
                                        .put("$ref", "entity")
                                        .put("$id", "507f1f77bcf86cd799439017")))));

        // Field Medic
        documents.add(new JsonObject()
                .put("_id", "507f1f77bcf86cd799439017")
                .put("type", "MapUnit")
                .put("version", 1)
                .put("state", new JsonObject()
                        .put("name", "Field Medic")
                        .put("offense", new JsonObject()
                                .put("burn", false)
                                .put("AP", false)
                                .put("melee", false)
                                .put("damage", new JsonArray().add(1)))
                        .put("position", new JsonObject()
                                .put("$ref", "entity")
                                .put("$id", "507f1f77bcf86cd799439021"))
                        .put("statuses", new JsonArray().add("healing"))
                        .put("zone", new JsonObject()
                                .put("$ref", "entity")
                                .put("$id", "507f1f77bcf86cd799439013"))));

        // Medic Position Vector
        documents.add(new JsonObject()
                .put("_id", "507f1f77bcf86cd799439021")
                .put("type", "MapVector2")
                .put("version", 1)
                .put("state", new JsonObject()
                        .put("owner", new JsonObject()
                                .put("$ref", "entity")
                                .put("$id", "507f1f77bcf86cd799439017"))
                        .put("x", 200.0)
                        .put("y", 200.0)));

        // Combat Zone
        documents.add(new JsonObject()
                .put("_id", "507f1f77bcf86cd799439012")
                .put("type", "Zone")
                .put("version", 1)
                .put("state", new JsonObject()
                        .put("buildings", new JsonArray()
                                .add(new JsonObject()
                                        .put("$ref", "entity")
                                        .put("$id", "507f1f77bcf86cd799439014"))
                                .add(new JsonObject()
                                        .put("$ref", "entity")
                                        .put("$id", "507f1f77bcf86cd799439015")))
                        .put("name", "Combat Zone")
                        .put("region", new JsonObject()
                                .put("$ref", "entity")
                                .put("$id", "507f1f77bcf86cd799439011"))
                        .put("units", new JsonArray()
                                .add(new JsonObject()
                                        .put("$ref", "entity")
                                        .put("$id", "507f1f77bcf86cd799439016")))));

        // Barracks
        documents.add(new JsonObject()
                .put("_id", "507f1f77bcf86cd799439015")
                .put("type", "Building")
                .put("version", 1)
                .put("state", new JsonObject()
                        .put("name", "Barracks")
                        .put("position", new JsonObject()
                                .put("$ref", "entity")
                                .put("$id", "507f1f77bcf86cd799439019"))
                        .put("statuses", new JsonArray())
                        .put("zone", new JsonObject()
                                .put("$ref", "entity")
                                .put("$id", "507f1f77bcf86cd799439012"))));

        // Barracks Position Vector
        //documents.add(new JsonObject()
        //        .put("_id", "507f1f77bcf86cd799439019")
        //        .put("type", "MapVector2")
        //        .put("version", 1)
        //        .put("state", new JsonObject()
        //                .put("owner", new JsonObject()
        //                        .put("$ref", "entity")
        //                        .put("$id", "507f1f77bcf86cd799439015"))
        //                .put("x", 150.0)
        //                .put("y", 100.0)));

        // Headquarters
        documents.add(new JsonObject()
                .put("_id", "507f1f77bcf86cd799439014")
                .put("type", "Building")
                .put("version", 1)
                .put("state", new JsonObject()
                        .put("name", "Headquarters")
                        .put("position", new JsonObject()
                                .put("$ref", "entity")
                                .put("$id", "507f1f77bcf86cd799439018"))
                        .put("statuses", new JsonArray()
                                .add("powered")
                                .add("fortified"))
                        .put("zone", new JsonObject()
                                .put("$ref", "entity")
                                .put("$id", "507f1f77bcf86cd799439012"))));

        // HQ Position Vector
        //documents.add(new JsonObject()
        //        .put("_id", "507f1f77bcf86cd799439018")
        //        .put("type", "MapVector2")
        //        .put("version", 1)
        //        .put("state", new JsonObject()
        //                .put("owner", new JsonObject()
        //                        .put("$ref", "entity")
        //                        .put("$id", "507f1f77bcf86cd799439014"))
        //                .put("x", 100.0)
        //                .put("y", 100.0)));

        // Elite Soldier
        documents.add(new JsonObject()
                .put("_id", "507f1f77bcf86cd799439016")
                .put("type", "MapUnit")
                .put("version", 1)
                .put("state", new JsonObject()
                        .put("name", "Elite Soldier")
                        .put("offense", new JsonObject()
                                .put("burn", true)
                                .put("AP", false)
                                .put("melee", true)
                                .put("damage", new JsonArray().add(5).add(10).add(15)))
                        .put("position", new JsonObject()
                                .put("$ref", "entity")
                                .put("$id", "507f1f77bcf86cd799439020"))
                        .put("statuses", new JsonArray()
                                .add("alert")
                                .add("armed"))
                        .put("zone", new JsonObject()
                                .put("$ref", "entity")
                                .put("$id", "507f1f77bcf86cd799439012"))));

        // Soldier Position Vector
        documents.add(new JsonObject()
                .put("_id", "507f1f77bcf86cd799439020")
                .put("type", "MapVector2")
                .put("version", 1)
                .put("state", new JsonObject()
                        .put("owner", new JsonObject()
                                .put("$ref", "entity")
                                .put("$id", "507f1f77bcf86cd799439016"))
                        .put("x", 120.0)
                        .put("y", 120.0)));

        return documents;
    }

    public JsonArray createLookupGraphQuery() {
        return new JsonArray();
    }

    //public Graph<Entity, DefaultEdge> createAncestorGraph() {
    //    return new DefaultDirectedGraph<Entity, DefaultEdge>();
    //}

    public JsonArray createUpdateQuery() {
        return new JsonArray();
    }

    @Test
    void testSerialize_success(VertxTestContext testContext) {
        Region region = createTestFixture();
        JsonArray expected = createTestJson();

        // Keep hash printing for determinism check
        String expectedHash = hashJsonObject(expected);

        region.serialize().onComplete(ar -> {
            if (ar.succeeded()) {
                JsonArray serialized = ar.result();
                String actualHash = hashJsonObject(serialized);

                testContext.verify(() -> {
                    // Print hashes for debugging
                    System.out.println("Expected hash: " + expectedHash);
                    System.out.println("Actual hash: " + actualHash);

                    // AssertJ deep comparison
                    assertThat(serialized)
                            .usingRecursiveComparison()
                            .isEqualTo(expected);

                    // Also verify hash equality for determinism
                    assertThat(actualHash).isEqualTo(expectedHash);

                    testContext.completeNow();
                });
            } else {
                testContext.failNow(ar.cause());
            }
        });
    }

    @Test
    void testUpdate_success(VertxTestContext testContext) {

    }

    private String hashJsonObject(JsonArray json) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(json.encode().getBytes());
            StringBuilder hexString = new StringBuilder();
            for (byte b : hash) {
                String hex = Integer.toHexString(0xff & b);
                if (hex.length() == 1) hexString.append('0');
                hexString.append(hex);
            }
            return hexString.toString();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}

@JsonTypeName("Region")
@EntityType("Region")
class Region extends Entity {
    @State
    @JsonProperty("zones")
    public List<Zone> zones;

    @State
    @JsonProperty("name")
    public String name;
}

@JsonTypeName("Zone")
@EntityType("Zone")
class Zone extends Entity {
    @JsonProperty("name")
    @State
    public String name;

    @State
    @JsonProperty("region")
    @ParentReference
    public Region region;  // back-reference

    @JsonProperty("units")
    @State
    @MutateStrict
    @ChildReference
    public List<MapUnit> units;

    @JsonProperty("buildings")
    @State
    @ChildReference
    public List<Building> buildings;
}

@JsonTypeName("Building")
@EntityType("Building")
class Building extends Entity {
    @JsonProperty("name")
    @State
    public String name;

    @JsonProperty("zone")
    @State
    public Zone zone;  // back-reference

    @JsonProperty("position")
    @State
    @ChildReference
    public MapVector2 position;

    @JsonProperty("statuses")
    @State
    public Set<String> statuses;
}

@JsonTypeName("MapUnit")
@EntityType("MapUnit")
class MapUnit extends Entity {
    @JsonProperty("name")
    @State
    public String name;

    @JsonProperty("zone")
    @State
    public Zone zone;  // back-reference

    @JsonProperty("position")
    @State
    @ChildReference
    public MapVector2 position;

    @JsonProperty("statuses")
    @State
    public Set<String> statuses;

    @JsonProperty("offense")
    @State
    public JsonObject offense;  // contains burn, AP, melee booleans and int array
}

@JsonTypeName("MapVector2")
@EntityType("MapVector2")
class MapVector2 extends Entity {
    @JsonProperty("x")
    @State
    public double x;

    @JsonProperty("y")
    @State
    public double y;

    @JsonProperty("owner")
    @State
    @ParentReference
    public MapUnit owner;  // back-reference to either Building or MapUnit
}

class TestEntityFactory implements EntityFactory {
    private final Map<String, Class<?>> classes;

    public TestEntityFactory() {
        classes = new HashMap<>();
        // Register our base types
        classes.put("region", Region.class);
        classes.put("zone", Zone.class);
        classes.put("building", Building.class);
        classes.put("mapunit", MapUnit.class);
        classes.put("mapvector2", MapVector2.class);
    }

    @Override
    public Class<?> useClass(String type) {
        return classes.get(type.toLowerCase());
    }

    @Override
    public Entity getNew(String type) {
        switch (type.toLowerCase()) {
            case "region":
                return new Region();
            case "zone":
                return new Zone();
            case "building":
                return new Building();
            case "mapunit":
                return new MapUnit();
            case "mapvector2":
                return new MapVector2();
            default:
                return new Entity();
        }
    }
}