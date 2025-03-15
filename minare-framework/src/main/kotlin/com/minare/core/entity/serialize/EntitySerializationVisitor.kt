package com.minare.core.entity.serialize

import com.minare.core.models.Entity
import com.minare.core.models.annotations.entity.EntityType
import com.minare.core.models.annotations.entity.State
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import java.lang.reflect.Field

class EntitySerializationVisitor {
    val documents = JsonArray()

    fun visit(entity: Entity) {
        val document = JsonObject()
                .put("_id", entity._id)
                .put("type", entity.javaClass.getAnnotation(_root_ide_package_.com.minare.core.models.annotations.entity.EntityType::class.java).value)
            .put("version", 1)  // For now hardcoded since we haven't implemented versioning
                .put("state", serializeState(entity))

        documents.add(document)
    }

    private fun serializeState(entity: Entity): JsonObject {
        val state = JsonObject()
        val stateFields = mutableListOf<Field>()

        // Collect all state fields first
        entity.javaClass.declaredFields.forEach { field ->
            if (field.isAnnotationPresent(_root_ide_package_.com.minare.core.models.annotations.entity.State::class.java)) {
                stateFields.add(field)
            }
        }

        // Sort fields by name
        stateFields.sortBy { it.name }

        // Add fields in sorted order
        stateFields.forEach { field ->
                field.isAccessible = true
            try {
                val value = field.get(entity)
                addFieldToState(state, field.name, value)
            } catch (e: IllegalAccessException) {
                throw RuntimeException("Failed to access field: ${field.name}", e)
            }
        }

        return state
    }

    private fun addFieldToState(state: JsonObject, fieldName: String, value: Any?) {
        if (value == null) {
            return  // Skip null values
        }

        when (value) {
            is Entity -> {
                // Entity reference
                state.put(fieldName, JsonObject()
                        .put("${'$'}ref", "entity")
                        .put("${'$'}id", value._id))
            }
            is Collection<*> -> {
                // Convert collection to JsonArray
                val array = JsonArray()
                value.forEach { item ->
                        when (item) {
                    is Entity -> {
                        array.add(JsonObject()
                                .put("${'$'}ref", "entity")
                                .put("${'$'}id", item._id))
                    }
                    is String -> {
                        array.add(item)  // For things like our status strings
                    }
                        else -> {
                        array.add(item)  // Basic types
                    }
                }
                }
                state.put(fieldName, array)
            }
            is JsonObject -> {
                // Already a JsonObject (like our offense field)
                state.put(fieldName, value)
            }
            else -> {
                // Basic types (numbers, strings, etc)
                state.put(fieldName, value)
            }
        }
    }
}