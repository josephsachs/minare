package com.minare.core.utils

import io.vertx.core.json.JsonObject

/**
 * Interface for custom types that can be serialized to/from JSON for Entity state storage.
 *
 * Types implementing this interface can be used as @State fields in Entity classes.
 * Deserialization leverages Jackson's mapTo() method, so implementing classes should be
 * data classes with properties matching their JSON representation.
 *
 * Example:
 * ```
 * data class Vector2(val x: Int, val y: Int) : JsonSerializable {
 *     fun toJson() = JsonObject().put("x", x).put("y", y)
 * }
 * ```
 */
interface JsonSerializable {
    /**
     * Serialize this object to a JsonObject for storage in Redis.
     * The resulting JSON structure should match the constructor parameters
     * to enable automatic deserialization via Jackson.
     */
    fun toJson(): JsonObject
}