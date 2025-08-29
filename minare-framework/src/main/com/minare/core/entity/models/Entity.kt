package com.minare.core.entity.models

import com.fasterxml.jackson.annotation.*

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonIgnoreProperties(ignoreUnknown = true)
open class Entity {
    @JsonProperty("version")
    var version: Long = 1

    @JsonProperty("_id")
    var _id: String? = null

    @JsonProperty("type")
    var type: String? = null

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Entity) return false
        return _id == other._id
    }

    override fun hashCode(): Int {
        return _id?.hashCode() ?: 0
    }
}