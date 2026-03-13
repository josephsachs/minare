package com.minare.core.operation.models

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import io.vertx.core.json.JsonObject
import java.io.Serializable
import java.util.UUID

/**
 * A condition check within an OperationSet.
 * The worker evaluates the assertion; on failure it applies the set's FailurePolicy.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class Assert : OperationSetMember, Serializable {
    var id: String = UUID.randomUUID().toString()

    @JsonProperty("entityId")
    var entity: String? = null
    var entityType: String? = null
    var values = JsonObject()
    var target: String = String()
    var timestamp: Long = System.currentTimeMillis()
    var meta: String? = null

    fun values(values: Map<String, Any?>) = apply {
        values.forEach { (k, v) -> this.values.put(k, v) }
    }

    @JsonProperty("entityId")
    fun entity(entity: String?) = apply {
        this.entity = entity
    }

    override fun build(): JsonObject {
        requireNotNull(entityType) { "Entity type is required" }
        require(target.isNotBlank()) { "Target condition name is required" }

        return JsonObject()
            .put("id", id)
            .put("entityId", entity)
            .put("entityType", entityType)
            .put("action", "ASSERT")
            .put("target", target)
            .put("values", values)
            .put("timestamp", timestamp)
            .also { json -> meta?.let { json.put("meta", it) } }
    }
}
