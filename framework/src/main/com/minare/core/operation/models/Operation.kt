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
class Operation: OperationSetStrategy, Serializable {
    var id: String = UUID.randomUUID().toString()
    @JsonProperty("entityId")
    var entity: String? = null
    var entityType: String? = null
    var action: OperationType? = null
    var values = JsonObject()
    var delta: JsonObject? = null
    var version: Long? = null
    var timestamp: Long = System.currentTimeMillis()
    var meta: String? = null

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
     * Accept a Java class for Operation creation
     */
    fun entityType(type: Class<*>) = apply {
        this.entityType = type.simpleName
    }

    /**
     * Accept a Kotlin class for Operation creation
     */
    fun entityType(type: KClass<*>) = apply {
        this.entityType = type.simpleName
            ?: throw IllegalArgumentException("Entity class must have a name")
    }

    /**
     * Deserialize entity type
     */
    private fun entityType(type: String) = apply {
        this.entityType = type
    }

    /**
     * Set the action type for this operation
     */
    fun action(action: OperationType) = apply {
        this.action = action
    }

    /**
     * Set the delta (changes) for this operation
     */
    fun delta(delta: JsonObject) = apply {
        this.delta = delta
    }

    /**
     * Set the version for optimistic locking
     */
    fun version(version: Long) = apply {
        this.version = version
    }

    /**
     * Set the timestamp for this operation
     */
    fun timestamp(timestamp: Long) = apply {
        this.timestamp = timestamp
    }

    fun meta(meta: String) = apply {
        this.meta = meta
    }

    /**
     * Add a key-value pair to the operation values
     */
    fun value(key: String, value: Any?) = apply {
        this.values.put(key, value)
    }

    /**
     * Add multiple key-value pairs to the operation values
     */
    fun values(values: Map<String, Any?>) = apply {
        values.forEach { (k, v) -> this.values.put(k, v) }
    }

    /**
     * Build the operation into a JsonObject
     */
    override fun build(): JsonObject {
        requireNotNull(entityType) { "Entity type is required" }
        requireNotNull(action) { "Action is required" }

        if (action == OperationType.MUTATE || action == OperationType.DELETE) {
            requireNotNull(entity) { "Entity ID is required" }
        }

        val operation = JsonObject()
            .put("id", id)
            .put("entityId", entity)
            .put("entityType", entityType)
            .put("action", action.toString())
            .put("timestamp", timestamp)

        version?.let { operation.put("version", it) }
        delta?.let { operation.put("delta", it) }
        meta?.let { operation.put("meta", it ) }
        operation.mergeIn(values)

        return operation
    }
}