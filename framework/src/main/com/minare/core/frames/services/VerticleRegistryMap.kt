package com.minare.core.frames.services

import com.hazelcast.map.IMap
import io.vertx.core.json.JsonObject

/**
 * Interface for the distributed verticle registry map.
 * Backs VerticleRegistry with Hazelcast for cluster-wide instance tracking.
 */
interface VerticleRegistryMap {
    fun get(instanceId: String): JsonObject?
    fun put(instanceId: String, state: JsonObject): JsonObject?
    fun remove(instanceId: String): JsonObject?
    fun clear()
    fun entries(): Set<Map.Entry<String, JsonObject>>
    fun values(): Collection<JsonObject>
}

/**
 * Hazelcast implementation of VerticleRegistryMap
 */
class HazelcastVerticleRegistryMap(
    private val map: IMap<String, JsonObject>
) : VerticleRegistryMap {

    override fun get(instanceId: String): JsonObject? = map.get(instanceId)

    override fun put(instanceId: String, state: JsonObject): JsonObject? = map.put(instanceId, state)

    override fun remove(instanceId: String): JsonObject? = map.remove(instanceId)

    override fun clear() = map.clear()

    override fun entries(): Set<Map.Entry<String, JsonObject>> = map.entries

    override fun values(): Collection<JsonObject> = map.values
}
