package com.minare.core.entity.services

import com.google.inject.Singleton
import com.minare.core.entity.models.Entity
import com.minare.core.utils.JsonSerializable
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

@Singleton
class EntityFieldSerializer {
    private val log = LoggerFactory.getLogger(EntityFieldSerializer::class.java)

    fun serialize(value: Any): Any {
        return when {
            // Entity reference - store just the ID
            value is Entity -> value._id

            // Enum - store the name as a string
            value is Enum<*> -> value.name

            // Collection - check if it contains entities
            value is Collection<*> -> {
                if (value.isEmpty()) {
                    JsonArray()
                } else if (value.all { it is Entity }) {
                    // Collection of entities - store IDs
                    JsonArray(value.filterIsInstance<Entity>().map { it._id })
                } else {
                    // Collection of primitives or data classes - store as-is
                    value
                }
            }

            // JsonObject - pass through
            value is JsonObject -> value
            value is JsonArray -> value

            // Primitives - pass through
            value is String || value is Number || value is Boolean -> value

            // JsonSerializable (legacy support during transition)
            value is JsonSerializable -> value.toJson()

            // Data classes and other serializable objects - use Jackson
            else -> {
                JsonObject(io.vertx.core.json.Json.encode(value))
            }
        }
    }
}