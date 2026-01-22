package com.minare.application.config

import com.minare.core.entity.factories.EntityFactory

class EntityValidator {

    fun validate(factoryClass: EntityFactory): Boolean {
        return false
    }

    fun isEntityValid(entity: Class<*>): Boolean {
        // Error for
        // - Nonserializable datatypes and collections
        // - Nonsense like websockets, threads, vertx instances and so on
        // - Cats - you can't serialize a cat!
        // Warn for
        // - Reference fields on DTO types - these will not be serialized as nested objects
        // - Entity references on DTO types - this feature doesn't exist yet but will
        // - Entity collections - this feature doesn't exist yet but will
        return false
    }
}