package com.minare.core.entity.services.json

import com.hazelcast.nio.ObjectDataInput
import com.hazelcast.nio.ObjectDataOutput
import com.hazelcast.nio.serialization.StreamSerializer
import io.vertx.core.json.JsonObject

/**
 * Hazelcast StreamSerializer for Vert.x JsonObject instances.
 * This allows JsonObject to be stored directly in Hazelcast maps without manual serialization.
 */
class JsonObjectSerializer : StreamSerializer<JsonObject> {

    companion object {
        // Must be unique across all serializers and > 0
        const val TYPE_ID = 1001
    }

    override fun getTypeId(): Int = TYPE_ID

    override fun write(out: ObjectDataOutput, obj: JsonObject) {
        val jsonString = obj.encode()
        out.writeString(jsonString)
    }

    override fun read(input: ObjectDataInput): JsonObject {
        val jsonString = input.readString()
        return JsonObject(jsonString)
    }

    override fun destroy() {
        // No resources to clean up
    }
}