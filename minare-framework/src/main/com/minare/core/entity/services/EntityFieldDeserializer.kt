package com.minare.core.entity.services

import com.google.inject.Singleton
import com.minare.core.entity.models.Entity
import com.minare.core.utils.JsonSerializable
import com.minare.exceptions.EntitySerializationException
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.lang.reflect.Field
import java.lang.reflect.ParameterizedType

@Singleton
class EntityFieldDeserializer {
    /**
     * Deserialize Entity field from Redis
     */
    fun deserialize(value: Any?, field: Field): Any? = when {
        value == null -> null

        field.type == String::class.java -> value
        field.type == Int::class.java -> (value as Number).toInt()
        field.type == Long::class.java -> (value as Number).toLong()
        field.type == Boolean::class.java -> value
        field.type.isEnum -> {
            field.type.enumConstants.first { (it as Enum<*>).name == value }
        }

        // Entity types - for now, just store the ID string
        // (Hydration will happen in a later ticket)
        Entity::class.java.isAssignableFrom(field.type) -> {
            // Value is a string ID, just return it for now
            // Future: hydrate from Redis
            value
        }

        value is JsonArray -> {
            deserializeCollection(value, field)
        }

        field.type == JsonObject::class.java -> {
            when (value) {
                is JsonObject -> value
                is String -> JsonObject(value)
                else -> value
            }
        }

        // JsonSerializable (legacy)
        JsonSerializable::class.java.isAssignableFrom(field.type) -> {
            when (value) {
                is JsonObject -> value.mapTo(field.type)
                is String -> JsonObject(value).mapTo(field.type)
                else -> throw EntitySerializationException(
                    "Cannot deserialize field '${field.name}' of type ${field.type.simpleName}: " +
                            "expected JsonObject or JSON String, got ${value?.javaClass?.simpleName}"
                )
            }
        }

        value is JsonObject -> {
            value.mapTo(field.type)
        }

        else -> {
            value
        }
    }

    private fun deserializeCollection(jsonArray: JsonArray, field: Field): Any? {
        val fieldType = field.type
        val genericType = field.genericType

        // Get the element type from generics
        val elementType = when (genericType) {
            is ParameterizedType -> genericType.actualTypeArguments[0] as? Class<*>
            else -> null
        } ?: Any::class.java

        // Convert JsonArray elements to the appropriate type
        val elements = jsonArray.map { element ->
            when {
                element == null -> null

                // Entity type - just return the ID string for now
                // (Hydration will happen in a later ticket)
                Entity::class.java.isAssignableFrom(elementType) -> element

                elementType.isEnum -> {
                    elementType.enumConstants.first { (it as Enum<*>).name == element }
                }
                element is JsonObject -> element.mapTo(elementType)
                element is JsonArray -> element.list
                else -> {
                    when (elementType) {
                        Int::class.java -> (element as Number).toInt()
                        Long::class.java -> (element as Number).toLong()
                        Double::class.java -> (element as Number).toDouble()
                        Float::class.java -> (element as Number).toFloat()
                        Boolean::class.java -> element as Boolean
                        String::class.java -> element.toString()
                        else -> element
                    }
                }
            }
        }

        // Return the appropriate collection type
        return when {
            List::class.java.isAssignableFrom(fieldType) -> elements.toMutableList()
            Set::class.java.isAssignableFrom(fieldType) -> elements.toMutableSet()
            fieldType.isArray -> {
                val array = java.lang.reflect.Array.newInstance(elementType, elements.size)
                elements.forEachIndexed { i, elem ->
                    java.lang.reflect.Array.set(array, i, elem)
                }
                array
            }
            Collection::class.java.isAssignableFrom(fieldType) -> elements
            else -> elements
        }
    }
}