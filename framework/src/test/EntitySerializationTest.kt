package com.minare.core.entity.services

import com.minare.core.entity.annotations.Property
import com.minare.core.entity.annotations.State
import com.minare.core.entity.models.Entity
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.assertj.core.api.Assertions.assertThat
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.util.*
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CopyOnWriteArraySet

enum class TestStatus {
    ACTIVE, INACTIVE, PENDING
}

data class TestVector(val x: Double = 0.0, val y: Double = 0.0)

data class TestMetadata(
    val createdAt: Long = 0L,
    val tags: List<String> = emptyList()
)

class TestChildEntity : Entity() {
    init { type = "TestChild" }

    @State
    var name: String = ""
}

@DisplayName("Serialization Test Entity")
class SerializationTestEntity : Entity() {
    init { type = "SerializationTest" }

    // Primitives
    @State var stringField: String = ""
    @State var intField: Int = 0
    @State var longField: Long = 0L
    @State var doubleField: Double = 0.0
    @State var floatField: Float = 0f
    @State var booleanField: Boolean = false
    @State var shortField: Short = 0
    @State var byteField: Byte = 0

    // Nullable primitives
    @State var nullableString: String? = null
    @State var nullableInt: Int? = null

    // Enum
    @State var statusField: TestStatus = TestStatus.ACTIVE

    // Data classes
    @State var vectorField: TestVector = TestVector(0.0, 0.0)
    @State var metadataField: TestMetadata = TestMetadata(0L, emptyList())

    // Entity reference
    @State var childEntity: TestChildEntity? = null
    @State var childEntityId: String? = null

    // JsonObject
    @State var jsonObjectField: JsonObject = JsonObject()

    // Lists - interface types
    @State var stringList: MutableList<String> = mutableListOf()
    @State var intList: MutableList<Int> = mutableListOf()
    @State var entityList: MutableList<TestChildEntity> = mutableListOf()
    @State var dataClassList: MutableList<TestVector> = mutableListOf()

    // Lists - concrete types
    @State var arrayListField: ArrayList<String> = arrayListOf()
    @State var linkedListField: LinkedList<String> = LinkedList()
    @State var vectorListField: Vector<String> = Vector()
    @State var copyOnWriteListField: CopyOnWriteArrayList<String> = CopyOnWriteArrayList()

    // Sets - interface types
    @State var stringSet: MutableSet<String> = mutableSetOf()
    @State var intSet: MutableSet<Int> = mutableSetOf()

    // Sets - concrete types
    @State var hashSetField: HashSet<String> = hashSetOf()
    @State var linkedHashSetField: LinkedHashSet<String> = linkedSetOf()
    @State var treeSetField: TreeSet<String> = sortedSetOf()
    @State var copyOnWriteSetField: CopyOnWriteArraySet<String> = CopyOnWriteArraySet()

    // Queues/Deques
    @State var arrayDequeField: ArrayDeque<String> = ArrayDeque()
    @State var priorityQueueField: PriorityQueue<String> = PriorityQueue()

    // Arrays
    @State var stringArrayField: Array<String> = arrayOf()
    @State var intArrayField: Array<Int> = arrayOf()

    // Properties (not state)
    @Property var propertyString: String = ""
    @Property var propertyList: MutableList<String> = mutableListOf()
}

@DisplayName("EntityFieldSerializer")
class EntityFieldSerializerTest {

    private lateinit var serializer: EntityFieldSerializer

    @BeforeEach
    fun setUp() {
        serializer = EntityFieldSerializer()
    }

    @Nested
    @DisplayName("Primitives")
    inner class Primitives {

        @Test
        fun `string passes through`() {
            val result = serializer.serialize("hello")
            assertThat(result).isEqualTo("hello")
        }

        @Test
        fun `int passes through`() {
            val result = serializer.serialize(42)
            assertThat(result).isEqualTo(42)
        }

        @Test
        fun `long passes through`() {
            val result = serializer.serialize(42L)
            assertThat(result).isEqualTo(42L)
        }

        @Test
        fun `double passes through`() {
            val result = serializer.serialize(3.14)
            assertThat(result).isEqualTo(3.14)
        }

        @Test
        fun `boolean passes through`() {
            val result = serializer.serialize(true)
            assertThat(result).isEqualTo(true)
        }
    }

    @Nested
    @DisplayName("Enums")
    inner class Enums {

        @Test
        fun `enum serializes to name string`() {
            val result = serializer.serialize(TestStatus.ACTIVE)
            assertThat(result).isEqualTo("ACTIVE")
        }
    }

    @Nested
    @DisplayName("Entities")
    inner class Entities {

        @Test
        fun `entity serializes to id`() {
            val entity = TestChildEntity().apply {
                _id = "child-123"
                name = "Test Child"
            }
            val result = serializer.serialize(entity)
            assertThat(result).isEqualTo("child-123")
        }
    }

    @Nested
    @DisplayName("Collections")
    inner class Collections {

        @Test
        fun `empty list serializes to empty JsonArray`() {
            val result = serializer.serialize(mutableListOf<String>())
            assertThat(result).isInstanceOf(JsonArray::class.java)
            assertThat((result as JsonArray).isEmpty).isTrue()
        }

        @Test
        fun `string list serializes to JsonArray`() {
            val result = serializer.serialize(mutableListOf("a", "b", "c"))
            assertThat(result).isInstanceOf(JsonArray::class.java)
            val array = result as JsonArray
            assertThat(array.size()).isEqualTo(3)
            assertThat(array.getString(0)).isEqualTo("a")
            assertThat(array.getString(1)).isEqualTo("b")
            assertThat(array.getString(2)).isEqualTo("c")
        }

        @Test
        fun `int list serializes to JsonArray`() {
            val result = serializer.serialize(mutableListOf(1, 2, 3))
            assertThat(result).isInstanceOf(JsonArray::class.java)
            val array = result as JsonArray
            assertThat(array.getInteger(0)).isEqualTo(1)
            assertThat(array.getInteger(1)).isEqualTo(2)
            assertThat(array.getInteger(2)).isEqualTo(3)
        }

        @Test
        fun `entity list serializes to JsonArray of ids`() {
            val entities = mutableListOf(
                TestChildEntity().apply { _id = "child-1" },
                TestChildEntity().apply { _id = "child-2" }
            )
            val result = serializer.serialize(entities)
            assertThat(result).isInstanceOf(JsonArray::class.java)
            val array = result as JsonArray
            assertThat(array.getString(0)).isEqualTo("child-1")
            assertThat(array.getString(1)).isEqualTo("child-2")
        }

        @Test
        fun `set serializes to JsonArray`() {
            val result = serializer.serialize(mutableSetOf("x", "y", "z"))
            assertThat(result).isInstanceOf(JsonArray::class.java)
            val array = result as JsonArray
            assertThat(array.size()).isEqualTo(3)
        }

        @Test
        fun `list with nulls serializes correctly`() {
            val list = mutableListOf<String?>("a", null, "c")
            val result = serializer.serialize(list)
            assertThat(result).isInstanceOf(JsonArray::class.java)
            val array = result as JsonArray
            assertThat(array.getString(0)).isEqualTo("a")
            assertThat(array.getValue(1)).isNull()
            assertThat(array.getString(2)).isEqualTo("c")
        }

        @Test
        fun `nested data class list serializes correctly`() {
            val vectors = mutableListOf(
                TestVector(1.0, 2.0),
                TestVector(3.0, 4.0)
            )
            val result = serializer.serialize(vectors)
            assertThat(result).isInstanceOf(JsonArray::class.java)
            val array = result as JsonArray
            assertThat(array.size()).isEqualTo(2)
            assertThat(array.getJsonObject(0).getDouble("x")).isEqualTo(1.0)
            assertThat(array.getJsonObject(0).getDouble("y")).isEqualTo(2.0)
        }
    }

    @Nested
    @DisplayName("Data Classes")
    inner class DataClasses {

        @Test
        fun `data class serializes to JsonObject`() {
            val vector = TestVector(10.0, 20.0)
            val result = serializer.serialize(vector)
            assertThat(result).isInstanceOf(JsonObject::class.java)
            val json = result as JsonObject
            assertThat(json.getDouble("x")).isEqualTo(10.0)
            assertThat(json.getDouble("y")).isEqualTo(20.0)
        }

        @Test
        fun `nested data class serializes correctly`() {
            val metadata = TestMetadata(12345L, listOf("tag1", "tag2"))
            val result = serializer.serialize(metadata)
            assertThat(result).isInstanceOf(JsonObject::class.java)
            val json = result as JsonObject
            assertThat(json.getLong("createdAt")).isEqualTo(12345L)
            assertThat(json.getJsonArray("tags").size()).isEqualTo(2)
        }
    }

    @Nested
    @DisplayName("JsonObject passthrough")
    inner class JsonObjectPassthrough {

        @Test
        fun `JsonObject passes through unchanged`() {
            val original = JsonObject().put("key", "value")
            val result = serializer.serialize(original)
            assertThat(result).isSameAs(original)
        }

        @Test
        fun `JsonArray passes through unchanged`() {
            val original = JsonArray().add("a").add("b")
            val result = serializer.serialize(original)
            assertThat(result).isSameAs(original)
        }
    }
}

@DisplayName("EntityFieldDeserializer")
class EntityFieldDeserializerTest {

    private lateinit var deserializer: EntityFieldDeserializer

    @BeforeEach
    fun setUp() {
        deserializer = EntityFieldDeserializer()
    }

    private inline fun <reified T> getField(name: String): java.lang.reflect.Field {
        return T::class.java.getDeclaredField(name)
    }

    @Nested
    @DisplayName("Null handling")
    inner class NullHandling {

        @Test
        fun `null returns null`() {
            val field = getField<SerializationTestEntity>("stringField")
            val result = deserializer.deserialize(null, field)
            assertThat(result).isNull()
        }
    }

    @Nested
    @DisplayName("Primitives")
    inner class Primitives {

        @Test
        fun `string deserializes`() {
            val field = getField<SerializationTestEntity>("stringField")
            val result = deserializer.deserialize("hello", field)
            assertThat(result).isEqualTo("hello")
        }

        @Test
        fun `int deserializes from Number`() {
            val field = getField<SerializationTestEntity>("intField")
            val result = deserializer.deserialize(42L, field)
            assertThat(result).isEqualTo(42)
        }

        @Test
        fun `long deserializes`() {
            val field = getField<SerializationTestEntity>("longField")
            val result = deserializer.deserialize(42, field)
            assertThat(result).isEqualTo(42L)
        }

        @Test
        fun `double deserializes`() {
            val field = getField<SerializationTestEntity>("doubleField")
            val result = deserializer.deserialize(3.14, field)
            assertThat(result).isEqualTo(3.14)
        }

        @Test
        fun `float deserializes`() {
            val field = getField<SerializationTestEntity>("floatField")
            val result = deserializer.deserialize(3.14, field)
            assertThat(result).isEqualTo(3.14f)
        }

        @Test
        fun `boolean deserializes`() {
            val field = getField<SerializationTestEntity>("booleanField")
            val result = deserializer.deserialize(true, field)
            assertThat(result).isEqualTo(true)
        }

        @Test
        fun `short deserializes`() {
            val field = getField<SerializationTestEntity>("shortField")
            val result = deserializer.deserialize(42, field)
            assertThat(result).isEqualTo(42.toShort())
        }

        @Test
        fun `byte deserializes`() {
            val field = getField<SerializationTestEntity>("byteField")
            val result = deserializer.deserialize(42, field)
            assertThat(result).isEqualTo(42.toByte())
        }
    }

    @Nested
    @DisplayName("Enums")
    inner class Enums {

        @Test
        fun `enum deserializes from name string`() {
            val field = getField<SerializationTestEntity>("statusField")
            val result = deserializer.deserialize("PENDING", field)
            assertThat(result).isEqualTo(TestStatus.PENDING)
        }
    }

    @Nested
    @DisplayName("Entity references")
    inner class EntityReferences {

        @Test
        fun `entity field returns id string unchanged`() {
            val field = getField<SerializationTestEntity>("childEntity")
            val result = deserializer.deserialize("child-123", field)
            assertThat(result).isEqualTo("child-123")
        }
    }

    @Nested
    @DisplayName("Data classes")
    inner class DataClasses {

        @Test
        fun `data class deserializes from JsonObject`() {
            val field = getField<SerializationTestEntity>("vectorField")
            val json = JsonObject().put("x", 10.0).put("y", 20.0)
            val result = deserializer.deserialize(json, field)
            assertThat(result).isInstanceOf(TestVector::class.java)
            val vector = result as TestVector
            assertThat(vector.x).isEqualTo(10.0)
            assertThat(vector.y).isEqualTo(20.0)
        }
    }

    @Nested
    @DisplayName("JsonObject fields")
    inner class JsonObjectFields {

        @Test
        fun `JsonObject field returns JsonObject unchanged`() {
            val field = getField<SerializationTestEntity>("jsonObjectField")
            val json = JsonObject().put("key", "value")
            val result = deserializer.deserialize(json, field)
            assertThat(result).isSameAs(json)
        }
    }

    @Nested
    @DisplayName("List interface types")
    inner class ListInterfaceTypes {

        @Test
        fun `MutableList String deserializes`() {
            val field = getField<SerializationTestEntity>("stringList")
            val array = JsonArray().add("a").add("b").add("c")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(MutableList::class.java)
            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<String>
            assertThat(list).containsExactly("a", "b", "c")
        }

        @Test
        fun `MutableList Int deserializes with type coercion`() {
            val field = getField<SerializationTestEntity>("intList")
            val array = JsonArray().add(1).add(2).add(3)
            val result = deserializer.deserialize(array, field)
            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<Int>
            assertThat(list).containsExactly(1, 2, 3)
        }

        @Test
        fun `empty MutableList deserializes`() {
            val field = getField<SerializationTestEntity>("stringList")
            val array = JsonArray()
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(MutableList::class.java)
            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<String>
            assertThat(list).isEmpty()
        }
    }

    @Nested
    @DisplayName("List concrete types")
    inner class ListConcreteTypes {

        @Test
        fun `ArrayList deserializes to ArrayList`() {
            val field = getField<SerializationTestEntity>("arrayListField")
            val array = JsonArray().add("a").add("b")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(ArrayList::class.java)
            @Suppress("UNCHECKED_CAST")
            val list = result as ArrayList<String>
            assertThat(list).containsExactly("a", "b")
        }

        @Test
        fun `LinkedList deserializes to LinkedList`() {
            val field = getField<SerializationTestEntity>("linkedListField")
            val array = JsonArray().add("x").add("y")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(LinkedList::class.java)
        }

        @Test
        fun `Vector deserializes to Vector`() {
            val field = getField<SerializationTestEntity>("vectorListField")
            val array = JsonArray().add("p").add("q")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(Vector::class.java)
        }

        @Test
        fun `CopyOnWriteArrayList deserializes to CopyOnWriteArrayList`() {
            val field = getField<SerializationTestEntity>("copyOnWriteListField")
            val array = JsonArray().add("a")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(CopyOnWriteArrayList::class.java)
        }
    }

    @Nested
    @DisplayName("Set interface types")
    inner class SetInterfaceTypes {

        @Test
        fun `MutableSet String deserializes`() {
            val field = getField<SerializationTestEntity>("stringSet")
            val array = JsonArray().add("a").add("b").add("a")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(MutableSet::class.java)
            @Suppress("UNCHECKED_CAST")
            val set = result as MutableSet<String>
            assertThat(set).containsExactlyInAnyOrder("a", "b")
        }
    }

    @Nested
    @DisplayName("Set concrete types")
    inner class SetConcreteTypes {

        @Test
        fun `HashSet deserializes to HashSet`() {
            val field = getField<SerializationTestEntity>("hashSetField")
            val array = JsonArray().add("a").add("b")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(HashSet::class.java)
        }

        @Test
        fun `LinkedHashSet deserializes to LinkedHashSet`() {
            val field = getField<SerializationTestEntity>("linkedHashSetField")
            val array = JsonArray().add("x").add("y")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(LinkedHashSet::class.java)
        }

        @Test
        fun `TreeSet deserializes to TreeSet`() {
            val field = getField<SerializationTestEntity>("treeSetField")
            val array = JsonArray().add("c").add("a").add("b")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(TreeSet::class.java)
            @Suppress("UNCHECKED_CAST")
            val set = result as TreeSet<String>
            assertThat(set.first()).isEqualTo("a")
        }

        @Test
        fun `CopyOnWriteArraySet deserializes to CopyOnWriteArraySet`() {
            val field = getField<SerializationTestEntity>("copyOnWriteSetField")
            val array = JsonArray().add("a")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(CopyOnWriteArraySet::class.java)
        }
    }

    @Nested
    @DisplayName("Queue and Deque types")
    inner class QueueDequeTypes {

        @Test
        fun `ArrayDeque deserializes to ArrayDeque`() {
            val field = getField<SerializationTestEntity>("arrayDequeField")
            val array = JsonArray().add("a").add("b")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(ArrayDeque::class.java)
        }

        @Test
        fun `PriorityQueue deserializes to PriorityQueue`() {
            val field = getField<SerializationTestEntity>("priorityQueueField")
            val array = JsonArray().add("c").add("a").add("b")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(PriorityQueue::class.java)
        }
    }

    @Nested
    @DisplayName("Array types")
    inner class ArrayTypes {

        @Test
        fun `String array deserializes`() {
            val field = getField<SerializationTestEntity>("stringArrayField")
            val array = JsonArray().add("a").add("b")
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(Array<String>::class.java)
            @Suppress("UNCHECKED_CAST")
            val arr = result as Array<String>
            assertThat(arr).containsExactly("a", "b")
        }

        @Test
        fun `Int array deserializes`() {
            val field = getField<SerializationTestEntity>("intArrayField")
            val array = JsonArray().add(1).add(2).add(3)
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(Array<Int>::class.java)
        }
    }

    @Nested
    @DisplayName("Collections with data class elements")
    inner class DataClassCollections {

        @Test
        fun `list of data classes deserializes`() {
            val field = getField<SerializationTestEntity>("dataClassList")
            val array = JsonArray()
                .add(JsonObject().put("x", 1.0).put("y", 2.0))
                .add(JsonObject().put("x", 3.0).put("y", 4.0))
            val result = deserializer.deserialize(array, field)
            assertThat(result).isInstanceOf(MutableList::class.java)
            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<TestVector>
            assertThat(list).hasSize(2)
            assertThat(list[0].x).isEqualTo(1.0)
            assertThat(list[1].y).isEqualTo(4.0)
        }
    }
}

@DisplayName("Round-trip serialization")
class RoundTripSerializationTest {

    private lateinit var serializer: EntityFieldSerializer
    private lateinit var deserializer: EntityFieldDeserializer

    @BeforeEach
    fun setUp() {
        serializer = EntityFieldSerializer()
        deserializer = EntityFieldDeserializer()
    }

    private inline fun <reified T> getField(name: String): java.lang.reflect.Field {
        return T::class.java.getDeclaredField(name)
    }

    @Test
    fun `string round trips`() {
        val original = "hello world"
        val serialized = serializer.serialize(original)
        val field = getField<SerializationTestEntity>("stringField")
        val deserialized = deserializer.deserialize(serialized, field)
        assertThat(deserialized).isEqualTo(original)
    }

    @Test
    fun `enum round trips`() {
        val original = TestStatus.PENDING
        val serialized = serializer.serialize(original)
        val field = getField<SerializationTestEntity>("statusField")
        val deserialized = deserializer.deserialize(serialized, field)
        assertThat(deserialized).isEqualTo(original)
    }

    @Test
    fun `string list round trips`() {
        val original = mutableListOf("a", "b", "c")
        val serialized = serializer.serialize(original)
        val field = getField<SerializationTestEntity>("stringList")
        val deserialized = deserializer.deserialize(serialized, field)
        assertThat(deserialized).isEqualTo(original)
    }

    @Test
    fun `empty list round trips`() {
        val original = mutableListOf<String>()
        val serialized = serializer.serialize(original)
        val field = getField<SerializationTestEntity>("stringList")
        val deserialized = deserializer.deserialize(serialized, field)
        assertThat(deserialized).isEqualTo(original)
    }

    @Test
    fun `data class round trips`() {
        val original = TestVector(10.0, 20.0)
        val serialized = serializer.serialize(original)
        val field = getField<SerializationTestEntity>("vectorField")
        val deserialized = deserializer.deserialize(serialized, field)
        assertThat(deserialized).isEqualTo(original)
    }

    @Test
    fun `ArrayList round trips as ArrayList`() {
        val original = arrayListOf("a", "b", "c")
        val serialized = serializer.serialize(original)
        val field = getField<SerializationTestEntity>("arrayListField")
        val deserialized = deserializer.deserialize(serialized, field)
        assertThat(deserialized).isInstanceOf(ArrayList::class.java)
        assertThat(deserialized).isEqualTo(original)
    }

    @Test
    fun `HashSet round trips as HashSet`() {
        val original = hashSetOf("x", "y", "z")
        val serialized = serializer.serialize(original)
        val field = getField<SerializationTestEntity>("hashSetField")
        val deserialized = deserializer.deserialize(serialized, field)
        assertThat(deserialized).isInstanceOf(HashSet::class.java)
        assertThat(deserialized).isEqualTo(original)
    }

    @Test
    fun `entity list round trips as id list`() {
        val entities = mutableListOf(
            TestChildEntity().apply { _id = "child-1"; name = "First" },
            TestChildEntity().apply { _id = "child-2"; name = "Second" }
        )
        val serialized = serializer.serialize(entities)

        assertThat(serialized).isInstanceOf(JsonArray::class.java)
        val array = serialized as JsonArray
        assertThat(array.getString(0)).isEqualTo("child-1")
        assertThat(array.getString(1)).isEqualTo("child-2")

        val field = getField<SerializationTestEntity>("entityList")
        val deserialized = deserializer.deserialize(serialized, field)

        @Suppress("UNCHECKED_CAST")
        val list = deserialized as MutableList<String>
        assertThat(list).containsExactly("child-1", "child-2")
    }
}