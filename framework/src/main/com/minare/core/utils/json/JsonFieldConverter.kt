package com.minare.core.utils.json

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.lang.reflect.Field
import java.lang.reflect.ParameterizedType
import com.google.inject.Singleton

/**
 * Utility for converting JSON values to appropriate field types during entity deserialization.
 * Handles the common case where JSON arrays need to be converted to Java/Kotlin collection types.
 */
@Singleton
class JsonFieldConverter {

    private val primitiveToBoxed = mapOf(
        Int::class.javaPrimitiveType to Int::class.javaObjectType,
        Long::class.javaPrimitiveType to Long::class.javaObjectType,
        Double::class.javaPrimitiveType to Double::class.javaObjectType,
        Float::class.javaPrimitiveType to Float::class.javaObjectType,
        Boolean::class.javaPrimitiveType to Boolean::class.javaObjectType,
        Byte::class.javaPrimitiveType to Byte::class.javaObjectType,
        Short::class.javaPrimitiveType to Short::class.javaObjectType,
        Char::class.javaPrimitiveType to Char::class.javaObjectType
    )

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

        if (isTypeMatch(field.type, value)) return value

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

        if (value is JsonObject) {
            return value
        }

        throw IllegalArgumentException("Cannot convert ${value::class.java} to ${field.type}")
    }

    private fun isTypeMatch(fieldType: Class<*>, value: Any): Boolean {
        if (fieldType.isInstance(value)) return true

        val boxedType = primitiveToBoxed[fieldType]
        if (boxedType != null && boxedType.isInstance(value)) return true

        return false
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
                val typeArg = typeArguments[0]
                if (typeArg is Class<*>) {
                    return typeArg
                }
            }
        }
        return Any::class.java
    }

    private fun convertElement(element: Any?, targetType: Class<*>): Any? {
        if (element == null) return null
        if (targetType.isInstance(element)) return element

        val boxedTarget = primitiveToBoxed[targetType] ?: targetType

        return when (boxedTarget) {
            java.lang.String::class.java -> element.toString()
            java.lang.Integer::class.java -> {
                when (element) {
                    is Number -> element.toInt()
                    is String -> element.toIntOrNull()
                    else -> null
                }
            }
            java.lang.Long::class.java -> {
                when (element) {
                    is Number -> element.toLong()
                    is String -> element.toLongOrNull()
                    else -> null
                }
            }
            java.lang.Double::class.java -> {
                when (element) {
                    is Number -> element.toDouble()
                    is String -> element.toDoubleOrNull()
                    else -> null
                }
            }
            java.lang.Float::class.java -> {
                when (element) {
                    is Number -> element.toFloat()
                    is String -> element.toFloatOrNull()
                    else -> null
                }
            }
            java.lang.Boolean::class.java -> {
                when (element) {
                    is Boolean -> element
                    is String -> element.toBooleanStrictOrNull()
                    else -> null
                }
            }
            else -> element
        }
    }
}