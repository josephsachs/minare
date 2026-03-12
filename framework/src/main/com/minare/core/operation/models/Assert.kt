package com.minare.core.operation.models

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.inject.Inject
import io.vertx.core.json.JsonObject
import java.io.Serializable
import java.util.UUID
import kotlin.coroutines.CoroutineContext

/**
 * An Operation is a request to the frame coordinator for changes to entity state.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
class Assert: OperationSetStrategy, Serializable {
    var id: String = UUID.randomUUID().toString()
    @JsonProperty("entityId")
    var entity: String? = null
    var entityType: String? = null
    var pipe: JsonObject? = null
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

    enum class OperationSetAssertType {
        TRUE,
        EQUIVALENT,
        EXISTS
    }

    var type: OperationSetAssertType? = null
    var context: Any? = null

    var timestamp: Long = System.currentTimeMillis()
    var meta: String? = null

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

    fun type(type: OperationSetAssertType) {
        this.type = type
    }

    /**
     *
     */
    override fun build(): JsonObject {
        throw Exception("Not yet implemented")
    }
}