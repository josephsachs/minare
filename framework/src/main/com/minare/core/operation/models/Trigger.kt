package com.minare.core.operation.models

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.vertx.core.json.JsonObject
import java.io.Serializable
import java.util.UUID

/**
 * A parallel, fire-and-forget step in an OperationSet. Invokes a named function via
 * reflection on a hydrated entity context — same mechanism as FunctionCall, but the
 * pipeline does not wait for it to return. Result does not feed into pipe.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class Trigger : OperationSetMember, Serializable {
    var id: String = UUID.randomUUID().toString()

    @JsonProperty("entityId")
    var entity: String? = null
    var entityType: String? = null
    var timestamp: Long = System.currentTimeMillis()
    var meta: String? = null
    var values = JsonObject()

    fun values(values: Map<String, Any?>) = apply {
        values.forEach { (k, v) -> this.values.put(k, v) }
    }

    @JsonProperty("entityId")
    fun entity(entity: String?) = apply {
        this.entity = entity
    }

    override fun build(): JsonObject {
        throw UnsupportedOperationException("Trigger.build() not yet implemented")
    }
}
