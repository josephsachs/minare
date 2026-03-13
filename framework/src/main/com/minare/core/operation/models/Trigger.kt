package com.minare.core.operation.models

import io.vertx.core.json.JsonObject
import java.util.UUID
import kotlin.reflect.KClass

/**
 * An OperationSet member that fires a named method on the target entity
 * without suspending or returning to the set context. Intended for true
 * side-effects; the executor launches it and immediately advances.
 */
class Trigger {
    var id: String = UUID.randomUUID().toString()
    var entity: String? = null
    var entityType: String? = null
    var function: String? = null
    var args: JsonObject? = null

    fun entity(id: String) = apply { this.entity = id }

    fun entityType(type: KClass<*>) = apply {
        this.entityType = type.simpleName
            ?: throw IllegalArgumentException("Entity class must have a name")
    }

    fun entityType(type: Class<*>) = apply {
        this.entityType = type.simpleName
    }

    fun function(name: String) = apply { this.function = name }

    fun args(args: JsonObject) = apply { this.args = args }

    fun build(): JsonObject {
        requireNotNull(entity) { "Entity ID is required for Trigger" }
        requireNotNull(entityType) { "Entity type is required for Trigger" }
        requireNotNull(function) { "Function name is required for Trigger" }

        return JsonObject()
            .put("id", id)
            .put("entityId", entity)
            .put("entityType", entityType)
            .put("action", "TRIGGER")
            .put("function", function)
            .also { args?.let { a -> it.put("args", a) } }
    }
}
