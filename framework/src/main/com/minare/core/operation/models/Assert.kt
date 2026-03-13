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
    var type: AssertType? = null
    var context: Any? = null
    var timestamp: Long = System.currentTimeMillis()
    var meta: String? = null

    enum class AssertType {
        TRUE,
        EQUIVALENT,
        EXISTS
    }

    fun values(values: Map<String, Any?>) = apply {
        values.forEach { (k, v) -> this.values.put(k, v) }
    }

    @JsonProperty("entityId")
    fun entity(entity: String?) = apply {
        this.entity = entity
    }

    fun type(type: AssertType) = apply {
        this.type = type
    }

    override fun build(): JsonObject {
        throw UnsupportedOperationException("Assert.build() not yet implemented")
    }
}
