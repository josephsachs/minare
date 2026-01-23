package com.minare.core.frames.services

import com.hazelcast.map.IMap
import io.vertx.core.json.JsonObject

/**
 * Interface for the distributed worker registry map.
 * This abstraction allows for cleaner dependency injection and potential
 * future implementations (e.g., for testing or different clustering solutions).
 */
interface WorkerRegistryMap {
    fun get(workerId: String): JsonObject?
    fun put(workerId: String, state: JsonObject): JsonObject?
    fun remove(workerId: String): JsonObject?
    fun clear()
    fun entries(): Set<Map.Entry<String, JsonObject>>
    fun values(): Collection<JsonObject>
}

/**
 * Hazelcast implementation of WorkerRegistryMap
 */
class HazelcastWorkerRegistryMap(
    private val map: IMap<String, JsonObject>
) : WorkerRegistryMap {

    override fun get(workerId: String): JsonObject? = map.get(workerId)

    override fun put(workerId: String, state: JsonObject): JsonObject? = map.put(workerId, state)

    override fun remove(workerId: String): JsonObject? = map.remove(workerId)

    override fun clear() = map.clear()

    override fun entries(): Set<Map.Entry<String, JsonObject>> = map.entries

    override fun values(): Collection<JsonObject> = map.values
}