package com.minare.core.operation.models

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.vertx.core.json.JsonObject
import java.io.Serializable
import java.util.UUID

/**
 * A blocking step in an OperationSet that invokes a named function via reflection
 * on a hydrated entity context — see how the @Task system does it.
 *
 * The pipeline waits for the call to return before advancing to the next step.
 * The result feeds into pipe for consumption by downstream steps.
 *
 * No stipulations on what you do inside. You can hurt yourself.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class FunctionCall : OperationSetMember, Serializable {
    var id: String = UUID.randomUUID().toString()

    @JsonProperty("entityId")
    var entity: String? = null
    var entityType: String? = null
    var target: String? = String()
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
        throw UnsupportedOperationException("FunctionCall.build() not yet implemented")
    }
}
