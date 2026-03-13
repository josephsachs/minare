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
    var values = JsonObject()
    var target: String = String()

    fun values(values: Map<String, Any?>) = apply {
        values.forEach { (k, v) -> this.values.put(k, v) }
    }

    @JsonProperty("entityId")
    fun entity(entity: String?) = apply {
        this.entity = entity
    }

    override fun build(): JsonObject {
        requireNotNull(entityType) { "Entity type is required" }
        require(target.isNotBlank()) { "Target function name is required" }

        return JsonObject()
            .put("id", id)
            .put("entityId", entity)
            .put("entityType", entityType)
            .put("action", "TRIGGER")
            .put("target", target)
            .put("values", values)
            .put("timestamp", timestamp)
    }
}
