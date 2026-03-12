package com.minare.core.operation.models

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.vertx.core.json.JsonObject
import java.io.Serializable
import java.util.UUID

/**
 * A request to invoke a named function on an entity within a frame.
 * The target is resolved and called via reflection; see how the @Task system does it.
 *
 * pipe carries inter-step data when sequenced in an OperationSet — design TBD.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class Invocation : OperationSetMember, Serializable {
    var id: String = UUID.randomUUID().toString()

    @JsonProperty("entityId")
    var entity: String? = null
    var entityType: String? = null
    var target: String? = null
    var pipe: JsonObject? = null
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
        throw UnsupportedOperationException("Invocation.build() not yet implemented")
    }
}
