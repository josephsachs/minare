package com.minare.core.entity.models.serializable

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.minare.core.utils.JsonSerializable
import io.vertx.core.json.JsonObject
import java.io.Serializable

/**
 * Two-dimensional integer vector.
 *
 * Implements Serializable for use as Hazelcast map keys in DistributedGridMap.
 * Implements JsonSerializable for use as @State fields in Entity classes.
 */
data class Vector2 @JsonCreator constructor(
    @JsonProperty("x") val x: Int,
    @JsonProperty("y") val y: Int
) : Serializable, JsonSerializable {
    override fun toJson(): JsonObject = JsonObject()
        .put("x", x)
        .put("y", y)
}