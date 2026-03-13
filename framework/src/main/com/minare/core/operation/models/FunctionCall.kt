package com.minare.core.operation.models

import io.vertx.core.json.JsonObject
import java.util.UUID
import kotlin.reflect.KClass

/**
 * An OperationSet member that suspends to invoke a named method on a hydrated
 * instance of the target entity. The return value becomes the step context
 * available to the next step.
 */
class FunctionCall {
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
        requireNotNull(entity) { "Entity ID is required for FunctionCall" }
        requireNotNull(entityType) { "Entity type is required for FunctionCall" }
        requireNotNull(function) { "Function name is required for FunctionCall" }

        return JsonObject()
            .put("id", id)
            .put("entityId", entity)
            .put("entityType", entityType)
            .put("action", "FUNCTION_CALL")
            .put("function", function)
            .also { args?.let { a -> it.put("args", a) } }
    }
}
