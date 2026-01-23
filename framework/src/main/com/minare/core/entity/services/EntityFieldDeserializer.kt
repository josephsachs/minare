package com.minare.core.entity.services

import com.google.inject.Singleton
import com.minare.core.entity.models.Entity
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.lang.reflect.Field
import java.lang.reflect.ParameterizedType
import java.util.*
import java.util.concurrent.ConcurrentLinkedDeque
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.CopyOnWriteArraySet

@Singleton
class EntityFieldDeserializer {

    fun deserialize(value: Any?, field: Field): Any? {
        if (value == null) return null

        return when {
            value is JsonArray -> deserializeCollection(value, field)
            value is JsonObject -> deserializeObject(value, field)
            else -> deserializePrimitive(value, field)
        }
    }

    private fun deserializeCollection(jsonArray: JsonArray, field: Field): Any {
        val fieldType = field.type
        val elementType = getElementType(field)
        val elements = jsonArray.map { deserializeElement(it, elementType) }

        return createCollection(fieldType, elements)
            ?: throw IllegalArgumentException("Cannot deserialize JsonArray to ${fieldType.name}")
    }

    private fun deserializeObject(jsonObject: JsonObject, field: Field): Any {
        val fieldType = field.type

        return when {
            fieldType == JsonObject::class.java -> jsonObject
            else -> jsonObject.mapTo(fieldType)
        }
    }

    private fun deserializePrimitive(value: Any, field: Field): Any {
        val fieldType = field.type

        return when {
            fieldType == String::class.java -> value.toString()
            fieldType == Int::class.java || fieldType == Int::class.javaObjectType -> (value as Number).toInt()
            fieldType == Long::class.java || fieldType == Long::class.javaObjectType -> (value as Number).toLong()
            fieldType == Double::class.java || fieldType == Double::class.javaObjectType -> (value as Number).toDouble()
            fieldType == Float::class.java || fieldType == Float::class.javaObjectType -> (value as Number).toFloat()
            fieldType == Boolean::class.java || fieldType == Boolean::class.javaObjectType -> value
            fieldType == Short::class.java || fieldType == Short::class.javaObjectType -> (value as Number).toShort()
            fieldType == Byte::class.java || fieldType == Byte::class.javaObjectType -> (value as Number).toByte()
            fieldType.isEnum -> fieldType.enumConstants.first { (it as Enum<*>).name == value }
            Entity::class.java.isAssignableFrom(fieldType) -> value
            else -> value
        }
    }

    private fun deserializeElement(element: Any?, elementType: Class<*>): Any? {
        if (element == null) return null

        return when {
            elementType == String::class.java -> element.toString()
            elementType == Int::class.java || elementType == Int::class.javaObjectType -> (element as Number).toInt()
            elementType == Long::class.java || elementType == Long::class.javaObjectType -> (element as Number).toLong()
            elementType == Double::class.java || elementType == Double::class.javaObjectType -> (element as Number).toDouble()
            elementType == Float::class.java || elementType == Float::class.javaObjectType -> (element as Number).toFloat()
            elementType == Boolean::class.java || elementType == Boolean::class.javaObjectType -> element
            elementType == Short::class.java || elementType == Short::class.javaObjectType -> (element as Number).toShort()
            elementType == Byte::class.java || elementType == Byte::class.javaObjectType -> (element as Number).toByte()
            elementType.isEnum -> elementType.enumConstants.first { (it as Enum<*>).name == element }
            Entity::class.java.isAssignableFrom(elementType) -> element
            element is JsonObject -> element.mapTo(elementType)
            element is JsonArray -> element.list
            else -> element
        }
    }

    private fun createCollection(fieldType: Class<*>, elements: List<Any?>): Any? {
        return createConcreteCollection(fieldType, elements)
            ?: createCollectionForInterface(fieldType, elements)
            ?: createArray(fieldType, elements)
    }

    private fun createConcreteCollection(fieldType: Class<*>, elements: List<Any?>): Any? {
        return when (fieldType) {
            // Lists
            ArrayList::class.java -> ArrayList(elements)
            LinkedList::class.java -> LinkedList(elements)
            Vector::class.java -> Vector(elements)
            Stack::class.java -> Stack<Any?>().apply { addAll(elements) }
            CopyOnWriteArrayList::class.java -> CopyOnWriteArrayList(elements)

            // Sets
            HashSet::class.java -> HashSet(elements)
            LinkedHashSet::class.java -> LinkedHashSet(elements)
            TreeSet::class.java -> TreeSet(elements.filterNotNull())
            CopyOnWriteArraySet::class.java -> CopyOnWriteArraySet(elements)

            // Queues/Deques
            ArrayDeque::class.java -> ArrayDeque(elements)
            PriorityQueue::class.java -> PriorityQueue(elements.filterNotNull())
            ConcurrentLinkedQueue::class.java -> ConcurrentLinkedQueue(elements)
            ConcurrentLinkedDeque::class.java -> ConcurrentLinkedDeque(elements)

            else -> null
        }
    }

    private fun createCollectionForInterface(fieldType: Class<*>, elements: List<Any?>): Any? {
        return when {
            // Sets - check before List since some sets implement other interfaces
            NavigableSet::class.java.isAssignableFrom(fieldType) -> TreeSet(elements.filterNotNull())
            SortedSet::class.java.isAssignableFrom(fieldType) -> TreeSet(elements.filterNotNull())
            MutableSet::class.java.isAssignableFrom(fieldType) -> HashSet(elements)
            Set::class.java.isAssignableFrom(fieldType) -> HashSet(elements)

            // Queues/Deques
            Deque::class.java.isAssignableFrom(fieldType) -> ArrayDeque(elements)
            Queue::class.java.isAssignableFrom(fieldType) -> LinkedList(elements)

            // Lists
            MutableList::class.java.isAssignableFrom(fieldType) -> ArrayList(elements)
            List::class.java.isAssignableFrom(fieldType) -> ArrayList(elements)

            // General
            MutableCollection::class.java.isAssignableFrom(fieldType) -> ArrayList(elements)
            Collection::class.java.isAssignableFrom(fieldType) -> ArrayList(elements)

            else -> null
        }
    }

    private fun createArray(fieldType: Class<*>, elements: List<Any?>): Any? {
        if (!fieldType.isArray) return null

        val componentType = fieldType.componentType
        val array = java.lang.reflect.Array.newInstance(componentType, elements.size)
        elements.forEachIndexed { i, elem ->
            java.lang.reflect.Array.set(array, i, elem)
        }
        return array
    }

    private fun getElementType(field: Field): Class<*> {
        val genericType = field.genericType
        if (genericType is ParameterizedType) {
            val typeArg = genericType.actualTypeArguments.firstOrNull()
            if (typeArg is Class<*>) {
                return typeArg
            }
        }
        if (field.type.isArray) {
            return field.type.componentType
        }
        return Any::class.java
    }
}