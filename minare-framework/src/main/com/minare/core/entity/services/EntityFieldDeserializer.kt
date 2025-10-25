package com.minare.core.entity.services

import com.google.inject.Singleton
import com.minare.core.entity.models.serializable.Vector2
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.lang.reflect.Field
import java.lang.reflect.ParameterizedType

@Singleton
class EntityFieldDeserializer {
    private val log = LoggerFactory.getLogger(EntityFieldDeserializer::class.java)
    /**
     * Deserialize Entity field from Redis
     */
    fun deserialize(value: Any?, field: Field): Any? = when {
        /**
        * Primitives
         */
        value == null -> null
        field.type == String::class.java -> value
        field.type == Int::class.java -> (value as Number).toInt()
        field.type == Long::class.java -> (value as Number).toLong()
        field.type == Boolean::class.java -> value
        field.type.isEnum -> {
            field.type.enumConstants.first { (it as Enum<*>).name == value }
        }
        /**
         * Helper types
         */
        field.type == Vector2::class.java -> {
            log.info("BEHAVIOR: $value is ${field.type}")
            if (value is Vector2) {
                Vector2(value.x, value.y)
            }
            Vector2
        }
        /**
         * Raw Json types
         */
        value is JsonArray -> {
            deserializeCollection(value, field)
        }
        value is JsonObject -> {
            // Direct JsonObject to object mapping
            value.mapTo(field.type)
        }
        else -> {
            // Complex types stored as JSON string
            JsonObject(value as String).mapTo(field.type)
        }
    }

    /**
     * Collections are handled as Json primitives for now
     * TODO: Deserialize helpers and Entity relationships
     */
    private fun deserializeCollection(jsonArray: JsonArray, field: Field): Any? {
        val fieldType = field.type
        val genericType = field.genericType

        // Get the element type from generics
        val elementType = when (genericType) {
            is ParameterizedType -> genericType.actualTypeArguments[0] as? Class<*>
            else -> null
        } ?: Any::class.java  // Default to Any if we can't determine type

        // Convert JsonArray elements to the appropriate type
        val elements = jsonArray.map { element ->
            when {
                element == null -> null
                elementType.isEnum -> {
                    elementType.enumConstants.first { (it as Enum<*>).name == element }
                }
                element is JsonObject -> element.mapTo(elementType)
                element is JsonArray -> element.list
                else -> {
                    // Primitives and strings
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
            List::class.java.isAssignableFrom(fieldType) -> elements
            Set::class.java.isAssignableFrom(fieldType) -> elements.toSet()
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