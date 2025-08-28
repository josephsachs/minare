package com.minare.core.utils.json

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.lang.reflect.Field
import java.lang.reflect.ParameterizedType
import javax.inject.Singleton

/**
 * Utility for converting JSON values to appropriate field types during entity deserialization.
 * Handles the common case where JSON arrays need to be converted to Java/Kotlin collection types.
 */
@Singleton
class JsonFieldConverter {

    /**
     * Converts a JSON value to the appropriate type for the given field.
     *
     * @param field The target field to set
     * @param value The JSON value to convert
     * @return The converted value ready to be set on the field
     * @throws IllegalArgumentException if the conversion is not supported
     */
    fun convert(field: Field, value: Any?): Any? {
        if (value == null) return null

        // If types already match, no conversion needed
        if (field.type.isInstance(value)) return value

        // Handle JsonArray to Collection conversions
        if (value is JsonArray) {
            return when {
                field.type == MutableList::class.java || field.type == ArrayList::class.java -> {
                    convertJsonArrayToMutableList(field, value)
                }
                field.type == List::class.java -> {
                    convertJsonArrayToList(field, value)
                }
                field.type == MutableSet::class.java || field.type == HashSet::class.java -> {
                    convertJsonArrayToMutableSet(field, value)
                }
                field.type == Set::class.java -> {
                    convertJsonArrayToSet(field, value)
                }
                field.type.isArray -> {
                    convertJsonArrayToArray(field, value)
                }
                else -> {
                    throw IllegalArgumentException("Cannot convert JsonArray to ${field.type}")
                }
            }
        }

        // Handle JsonObject conversions if needed in the future
        if (value is JsonObject) {
            // For now, just pass through - could add custom object mapping later
            return value
        }

        // If we get here, we don't know how to convert
        throw IllegalArgumentException("Cannot convert ${value::class.java} to ${field.type}")
    }

    private fun convertJsonArrayToMutableList(field: Field, jsonArray: JsonArray): MutableList<Any?> {
        val elementType = getCollectionElementType(field)
        return jsonArray.map { convertElement(it, elementType) }.toMutableList()
    }

    private fun convertJsonArrayToList(field: Field, jsonArray: JsonArray): List<Any?> {
        val elementType = getCollectionElementType(field)
        return jsonArray.map { convertElement(it, elementType) }
    }

    private fun convertJsonArrayToMutableSet(field: Field, jsonArray: JsonArray): MutableSet<Any?> {
        val elementType = getCollectionElementType(field)
        return jsonArray.map { convertElement(it, elementType) }.toMutableSet()
    }

    private fun convertJsonArrayToSet(field: Field, jsonArray: JsonArray): Set<Any?> {
        val elementType = getCollectionElementType(field)
        return jsonArray.map { convertElement(it, elementType) }.toSet()
    }

    private fun convertJsonArrayToArray(field: Field, jsonArray: JsonArray): Any {
        val elementType = field.type.componentType
        val array = java.lang.reflect.Array.newInstance(elementType, jsonArray.size())

        for (i in 0 until jsonArray.size()) {
            val convertedElement = convertElement(jsonArray.getValue(i), elementType)
            java.lang.reflect.Array.set(array, i, convertedElement)
        }

        return array
    }

    private fun getCollectionElementType(field: Field): Class<*> {
        val genericType = field.genericType
        if (genericType is ParameterizedType) {
            val typeArguments = genericType.actualTypeArguments
            if (typeArguments.isNotEmpty()) {
                return typeArguments[0] as Class<*>
            }
        }
        // Default to Object if we can't determine the element type
        return Any::class.java
    }

    private fun convertElement(element: Any?, targetType: Class<*>): Any? {
        if (element == null) return null
        if (targetType.isInstance(element)) return element

        // Handle basic type conversions
        return when (targetType) {
            String::class.java -> element.toString()
            Int::class.java, Integer::class.java -> {
                when (element) {
                    is Number -> element.toInt()
                    is String -> element.toIntOrNull()
                    else -> null
                }
            }
            Long::class.java -> {
                when (element) {
                    is Number -> element.toLong()
                    is String -> element.toLongOrNull()
                    else -> null
                }
            }
            Double::class.java -> {
                when (element) {
                    is Number -> element.toDouble()
                    is String -> element.toDoubleOrNull()
                    else -> null
                }
            }
            Boolean::class.java -> {
                when (element) {
                    is Boolean -> element
                    is String -> element.toBooleanStrictOrNull()
                    else -> null
                }
            }
            else -> {
                // For complex types, just pass through for now
                element
            }
        }
    }
}