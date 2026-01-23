package com.minare.core.operation.models

import com.minare.core.entity.models.Entity
import io.vertx.core.json.JsonObject
import java.util.UUID
import kotlin.reflect.KClass

/**
 * Represents a single operation to be processed.
 * Operations are the atomic units of work in the system.
 */
class Operation {
    private var id: String = UUID.randomUUID().toString()
    private var entity: String? = null
    private var entityType: String? = null
    private var action: OperationType? = null
    private var values = JsonObject()
    private var delta: JsonObject? = null
    private var version: Long? = null
    private var timestamp: Long = System.currentTimeMillis()
    private var meta: String? = null

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
     * Get the operation ID
     */
    fun getId(): String = id

    /**
     * Set the entity ID this operation targets
     */
    fun entity(entity: String) = apply {
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

    /***
     * Get the entity ID
     */
    fun getEntity(): String? = entity

    /**
     * Get the operation type
     */
    fun getAction(): OperationType? = action

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
    fun build(): JsonObject {
        requireNotNull(entity) { "Entity ID is required" }
        requireNotNull(entityType) { "Entity type is required" }
        requireNotNull(action) { "Action is required" }

        val operation = JsonObject()
            .put("id", id)
            .put("entityId", entity)
            .put("entityType", entityType)
            .put("action", action.toString())
            .put("timestamp", timestamp)

        // Add version if present
        version?.let { operation.put("version", it) }

        // Add delta if present
        delta?.let { operation.put("delta", it) }

        // Add meta if present
        meta?.let { operation.put("meta", it ) }

        // Merge any additional values
        operation.mergeIn(values)

        return operation
    }

    override fun toString(): String {
        return "Operation(id=$id, entity=$entity, action=$action)"
    }

    companion object {
        /**
         * Create an Operation from a JsonObject
         * Used when deserializing from Kafka or other sources
         */
        fun fromJson(json: JsonObject): Operation {
            val op = Operation()

            // Set ID if present, otherwise keep generated one
            json.getString("id")?.let { op.id(it) }

            // Required fields
            op.entity(json.getString("entityId")
                ?: throw IllegalArgumentException("Missing entityId"))
            op.entity(json.getString("entityType")
                ?: throw IllegalArgumentException("Missing entityType"))
            op.action(
                OperationType.valueOf(json.getString("action")
                ?: throw IllegalArgumentException("Missing action")))

            // Optional fields
            json.getLong("timestamp")?.let { op.timestamp(it) }
            json.getLong("version")?.let { op.version(it) }
            json.getJsonObject("delta")?.let { op.delta(it) }
            json.getString("meta")?.let { op.meta(it) }

            // Copy any other fields to values
            json.fieldNames()
                .filter { it !in setOf(
                    "id", "entityId", "entityType", "action", "timestamp", "version", "delta", "meta"
                ) }
                .forEach { key ->
                    op.value(key, json.getValue(key))
                }

            return op
        }
    }
}