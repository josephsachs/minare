package com.minare.core.utils.json

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.serialization.StreamSerializer
import io.vertx.core.json.JsonArray

/**
 * Hazelcast StreamSerializer for Vert.x JsonArray instances.
 * This allows JsonArray to be stored directly in Hazelcast maps without manual serialization.
 *
 * Optional: Only needed if you plan to store JsonArray instances in Hazelcast.
 */
class JsonArraySerializer : StreamSerializer<JsonArray> {

    companion object {
        // Must be unique across all serializers and > 0
        const val TYPE_ID = 1002
    }

    override fun getTypeId(): Int = TYPE_ID

    override fun write(out: ObjectDataOutput, obj: JsonArray) {
        // Convert JsonArray to its string representation
        val jsonString = obj.encode()
        out.writeString(jsonString)
    }

    override fun read(input: ObjectDataInput): JsonArray {
        // Read the string and reconstruct JsonArray
        val jsonString = input.readString()
        return JsonArray(jsonString)
    }

    override fun destroy() {
        // No resources to clean up
    }
}

// To enable JsonArray serialization, add this to HazelcastConfigFactory.configureJsonObjectSerialization():
/*
val jsonArraySerializerConfig = SerializerConfig()
    .setImplementation(JsonArraySerializer())
    .setTypeClass(JsonArray::class.java)

config.serializationConfig.addSerializerConfig(jsonArraySerializerConfig)
*/