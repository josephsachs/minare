package com.minare.core.entity.models

import com.fasterxml.jackson.annotation.*

open class Entity(
    var _id: String = "unsaved-${java.util.UUID.randomUUID()}"
) {
    var version: Long = 1
    var type: String = "Entity"

    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (other !is Entity) return false
        return _id == other._id
    }

    override fun hashCode(): Int {
        return _id.hashCode()
    }
}