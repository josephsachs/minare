package com.minare.core.operation.models

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.vertx.core.json.JsonObject
import java.io.Serializable
import java.util.UUID
import kotlin.reflect.KClass

/**
 * An Operation is a request to the frame coordinator for changes to entity state.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class Function: OperationSetStrategy, Serializable {
    var id: String = UUID.randomUUID().toString()

    @JsonProperty("entityId")
    var entity: String? = null
    var entityType: String? = null
    var target: String? = null // We will reflect-and-invoke this later; note how the @Task system does it
    var pipe: JsonObject? = null // Not sure about `pipe`; we might want to use something more like EventStateFlow, I welcome input
    var timestamp: Long = System.currentTimeMillis()
    var meta: String? = null
    var values = JsonObject()

    /**
     * Set the unique identifier for this operation.
     * By default, a UUID is generated, but this allows override if needed.
     */
    fun id(id: String) = apply {
        // Only allow override if not already built
        if (values.isEmpty) {
            this.id = id
        }
    }

    /**
     * Add multiple key-value pairs to the operation values
     */
    fun values(values: Map<String, Any?>) = apply {
        values.forEach { (k, v) -> this.values.put(k, v) }
    }

    /**
     * Set the entity ID this operation targets
     *
     * Must be transformed from entityId used by the fluent API
     * JsonProperty to assist ObjectMapper.mapTo
     */
    @JsonProperty("entityId")
    fun entity(entity: String?) = apply {
        this.entity = entity
    }

    /**
     *
     */
    override fun build(): JsonObject {
        throw Exception("Not yet implemented")
    }
}