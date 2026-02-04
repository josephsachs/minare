package com.minare.core.utils.json

import com.minare.core.entity.services.json.JsonFieldConverter
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.assertj.core.api.Assertions.assertThat
import org.assertj.core.api.Assertions.assertThatThrownBy
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Nested
import org.junit.jupiter.api.Test
import java.lang.reflect.Field

@DisplayName("JsonFieldConverter")
class JsonFieldConverterTest {

    private lateinit var converter: JsonFieldConverter

    class NullableFields {
        var nullableString: String? = null
        var nullableList: MutableList<String>? = null
    }

    class SimpleFields {
        var stringField: String = ""
        var intField: Int = 0
        var boolField: Boolean = false
    }

    class ListFields {
        var stringList: MutableList<String> = mutableListOf()
        var intList: MutableList<Int> = mutableListOf()
        var longList: MutableList<Long> = mutableListOf()
        var doubleList: MutableList<Double> = mutableListOf()
        var anyList: MutableList<Any> = mutableListOf()
    }

    class ArrayListFields {
        var stringArrayList: ArrayList<String> = arrayListOf()
    }

    class SetFields {
        var stringSet: Set<String> = setOf()
        var mutableStringSet: MutableSet<String> = mutableSetOf()
        var hashSet: HashSet<String> = hashSetOf()
        var intSet: MutableSet<Int> = mutableSetOf()
    }

    class ArrayFields {
        var stringArray: Array<String> = arrayOf()
        var intArray: Array<Int> = arrayOf()
    }

    class CoercionFields {
        var intField: Int = 0
        var longField: Long = 0L
        var doubleField: Double = 0.0
        var stringField: String = ""
        var boolField: Boolean = false
        var intList: MutableList<Int> = mutableListOf()
    }

    class JsonObjectFields {
        var jsonField: JsonObject = JsonObject()
    }

    class UnsupportedFields {
        var mapField: Map<String, String> = mapOf()
        var customField: StringBuilder = StringBuilder()
    }

    class ListInterfaceFields {
        var stringList: List<String> = listOf()
    }

    @BeforeEach
    fun setUp() {
        converter = JsonFieldConverter()
    }

    private inline fun <reified T> getField(name: String): Field {
        return T::class.java.getDeclaredField(name)
    }

    @Nested
    @DisplayName("Null Handling")
    inner class NullHandling {

        @Test
        fun `null value returns null`() {
            val field = getField<NullableFields>("nullableString")
            val result = converter.convert(field, null)
            assertThat(result).isNull()
        }

        @Test
        fun `null value for collection field returns null`() {
            val field = getField<NullableFields>("nullableList")
            val result = converter.convert(field, null)
            assertThat(result).isNull()
        }
    }

    @Nested
    @DisplayName("Type Match Passthrough")
    inner class TypeMatchPassthrough {

        @Test
        fun `string value for string field passes through`() {
            val field = getField<SimpleFields>("stringField")
            val result = converter.convert(field, "hello")
            assertThat(result).isEqualTo("hello")
        }

        @Test
        fun `int value for int field passes through`() {
            val field = getField<SimpleFields>("intField")
            val result = converter.convert(field, 42)
            assertThat(result).isEqualTo(42)
        }

        @Test
        fun `boolean value for boolean field passes through`() {
            val field = getField<SimpleFields>("boolField")
            val result = converter.convert(field, true)
            assertThat(result).isEqualTo(true)
        }
    }

    @Nested
    @DisplayName("JsonArray to MutableList")
    inner class JsonArrayToMutableList {

        @Test
        fun `JsonArray of strings to MutableList of String`() {
            val field = getField<ListFields>("stringList")
            val jsonArray = JsonArray().add("a").add("b").add("c")

            val result = converter.convert(field, jsonArray)

            assertThat(result).isInstanceOf(MutableList::class.java)
            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<String>
            assertThat(list).containsExactly("a", "b", "c")
        }

        @Test
        fun `JsonArray of numbers to MutableList of Int`() {
            val field = getField<ListFields>("intList")
            val jsonArray = JsonArray().add(1).add(2).add(3)

            val result = converter.convert(field, jsonArray)

            assertThat(result).isInstanceOf(MutableList::class.java)
            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<Int>
            assertThat(list).containsExactly(1, 2, 3)
        }

        @Test
        fun `JsonArray of numbers to MutableList of Long`() {
            val field = getField<ListFields>("longList")
            val jsonArray = JsonArray().add(1).add(2).add(3)

            val result = converter.convert(field, jsonArray)

            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<Long>
            assertThat(list).containsExactly(1L, 2L, 3L)
        }

        @Test
        fun `JsonArray of numbers to MutableList of Double`() {
            val field = getField<ListFields>("doubleList")
            val jsonArray = JsonArray().add(1.5).add(2.5).add(3.5)

            val result = converter.convert(field, jsonArray)

            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<Double>
            assertThat(list).containsExactly(1.5, 2.5, 3.5)
        }

        @Test
        fun `empty JsonArray to empty MutableList`() {
            val field = getField<ListFields>("stringList")
            val jsonArray = JsonArray()

            val result = converter.convert(field, jsonArray)

            assertThat(result).isInstanceOf(MutableList::class.java)
            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<*>
            assertThat(list).isEmpty()
        }

        @Test
        fun `JsonArray with nulls preserves nulls in MutableList`() {
            val field = getField<ListFields>("stringList")
            val jsonArray = JsonArray().add("a").addNull().add("c")

            val result = converter.convert(field, jsonArray)

            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<String?>
            assertThat(list).containsExactly("a", null, "c")
        }
    }

    @Nested
    @DisplayName("JsonArray to ArrayList")
    inner class JsonArrayToArrayList {

        @Test
        fun `JsonArray to ArrayList`() {
            val field = getField<ArrayListFields>("stringArrayList")
            val jsonArray = JsonArray().add("x").add("y")

            val result = converter.convert(field, jsonArray)

            assertThat(result).isInstanceOf(MutableList::class.java)
            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<String>
            assertThat(list).containsExactly("x", "y")
        }
    }

    @Nested
    @DisplayName("JsonArray to Set Types")
    inner class JsonArrayToSet {

        @Test
        fun `JsonArray to Set`() {
            val field = getField<SetFields>("stringSet")
            val jsonArray = JsonArray().add("a").add("b").add("a")

            val result = converter.convert(field, jsonArray)

            assertThat(result).isInstanceOf(Set::class.java)
            @Suppress("UNCHECKED_CAST")
            val set = result as Set<String>
            assertThat(set).containsExactlyInAnyOrder("a", "b")
        }

        @Test
        fun `JsonArray to MutableSet`() {
            val field = getField<SetFields>("mutableStringSet")
            val jsonArray = JsonArray().add("x").add("y")

            val result = converter.convert(field, jsonArray)

            assertThat(result).isInstanceOf(MutableSet::class.java)
            @Suppress("UNCHECKED_CAST")
            val set = result as MutableSet<String>
            assertThat(set).containsExactlyInAnyOrder("x", "y")
        }

        @Test
        fun `JsonArray to HashSet`() {
            val field = getField<SetFields>("hashSet")
            val jsonArray = JsonArray().add("p").add("q")

            val result = converter.convert(field, jsonArray)

            assertThat(result).isInstanceOf(MutableSet::class.java)
        }

        @Test
        fun `JsonArray of numbers to MutableSet of Int`() {
            val field = getField<SetFields>("intSet")
            val jsonArray = JsonArray().add(1).add(2).add(1)

            val result = converter.convert(field, jsonArray)

            @Suppress("UNCHECKED_CAST")
            val set = result as MutableSet<Int>
            assertThat(set).containsExactlyInAnyOrder(1, 2)
        }
    }

    @Nested
    @DisplayName("JsonArray to Array")
    inner class JsonArrayToArray {

        @Test
        fun `JsonArray to String Array`() {
            val field = getField<ArrayFields>("stringArray")
            val jsonArray = JsonArray().add("a").add("b")

            val result = converter.convert(field, jsonArray)

            assertThat(result).isInstanceOf(Array<String>::class.java)
            @Suppress("UNCHECKED_CAST")
            val array = result as Array<String>
            assertThat(array).containsExactly("a", "b")
        }

        @Test
        fun `JsonArray to Int Array`() {
            val field = getField<ArrayFields>("intArray")
            val jsonArray = JsonArray().add(1).add(2).add(3)

            val result = converter.convert(field, jsonArray)

            assertThat(result).isInstanceOf(Array<Int>::class.java)
        }
    }

    @Nested
    @DisplayName("Type Coercion")
    inner class TypeCoercion {

        @Test
        fun `Long number coerced to Int in collection`() {
            val field = getField<CoercionFields>("intList")
            val jsonArray = JsonArray().add(100L).add(200L)

            val result = converter.convert(field, jsonArray)

            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<Int>
            assertThat(list).containsExactly(100, 200)
        }

        @Test
        fun `Double number coerced to Int in collection truncates`() {
            val field = getField<CoercionFields>("intList")
            val jsonArray = JsonArray().add(1.9).add(2.1)

            val result = converter.convert(field, jsonArray)

            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<Int>
            assertThat(list).containsExactly(1, 2)
        }

        @Test
        fun `String number coerced to Int in collection`() {
            val field = getField<CoercionFields>("intList")
            val jsonArray = JsonArray().add("42").add("99")

            val result = converter.convert(field, jsonArray)

            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<Int>
            assertThat(list).containsExactly(42, 99)
        }

        @Test
        fun `Invalid string returns null for Int coercion`() {
            val field = getField<CoercionFields>("intList")
            val jsonArray = JsonArray().add("not-a-number")

            val result = converter.convert(field, jsonArray)

            @Suppress("UNCHECKED_CAST")
            val list = result as MutableList<Int?>
            assertThat(list).containsExactly(null)
        }
    }

    @Nested
    @DisplayName("JsonObject Handling")
    inner class JsonObjectHandling {

        @Test
        fun `JsonObject passes through for JsonObject field`() {
            val field = getField<JsonObjectFields>("jsonField")
            val jsonObject = JsonObject().put("key", "value")

            val result = converter.convert(field, jsonObject)

            assertThat(result).isInstanceOf(JsonObject::class.java)
            assertThat((result as JsonObject).getString("key")).isEqualTo("value")
        }
    }

    @Nested
    @DisplayName("Unsupported Conversions")
    inner class UnsupportedConversions {

        @Test
        fun `JsonArray to unsupported collection type throws`() {
            val field = getField<UnsupportedFields>("mapField")
            val jsonArray = JsonArray().add("a")

            assertThatThrownBy {
                converter.convert(field, jsonArray)
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("Cannot convert JsonArray")
        }

        @Test
        fun `Incompatible type conversion throws`() {
            val field = getField<UnsupportedFields>("customField")
            val value = 12345

            assertThatThrownBy {
                converter.convert(field, value)
            }.isInstanceOf(IllegalArgumentException::class.java)
                .hasMessageContaining("Cannot convert")
        }
    }

    @Nested
    @DisplayName("List Interface (immutable)")
    inner class ListInterface {

        @Test
        fun `JsonArray to List interface returns immutable list`() {
            val field = getField<ListInterfaceFields>("stringList")
            val jsonArray = JsonArray().add("a").add("b")

            val result = converter.convert(field, jsonArray)

            assertThat(result).isInstanceOf(List::class.java)
            @Suppress("UNCHECKED_CAST")
            val list = result as List<String>
            assertThat(list).containsExactly("a", "b")
        }
    }
}