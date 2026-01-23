package com.minare.core.entity.services

import com.google.inject.Singleton
import com.minare.core.entity.models.Entity
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

@Singleton
class EntityFieldSerializer {

    fun serialize(value: Any): Any {
        return when (value) {
            is Entity -> serializeEntity(value)
            is Enum<*> -> serializeEnum(value)
            is Collection<*> -> serializeCollection(value)
            is JsonObject, is JsonArray -> value
            is String, is Number, is Boolean -> value
            else -> serializeObject(value)
        }
    }

    private fun serializeEntity(entity: Entity): String = entity._id

    private fun serializeEnum(enum: Enum<*>): String = enum.name

    private fun serializeCollection(collection: Collection<*>): JsonArray {
        val array = JsonArray()
        for (item in collection) {
            if (item == null) {
                array.addNull()
            } else {
                array.add(serialize(item))
            }
        }
        return array
    }

    private fun serializeObject(value: Any): JsonObject {
        return JsonObject(io.vertx.core.json.Json.encode(value))
    }
}