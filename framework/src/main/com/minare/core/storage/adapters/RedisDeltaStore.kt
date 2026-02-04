package com.minare.core.storage.adapters

import com.minare.core.frames.models.FrameDelta
import com.minare.core.storage.interfaces.DeltaStore
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.Command
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.client.Response
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import com.google.inject.Singleton

@Singleton
class RedisDeltaStore @Inject constructor(
    private val redisAPI: RedisAPI
) : DeltaStore {
    private val log = LoggerFactory.getLogger(RedisDeltaStore::class.java)

    override suspend fun appendDelta(frameNumber: Long, delta: FrameDelta) {
        val key = "frame:${frameNumber}:deltas"

        val existing = redisAPI.jsonGet(listOf(key, "$")).await()
        val deltasArray = if (existing != null && existing.toString() != "null") {
            JsonArray(existing.toString().removePrefix("[").removeSuffix("]"))
        } else {
            JsonArray()
        }

        deltasArray.add(delta.toJson())

        redisAPI.jsonSet(listOf(key, "$", deltasArray.encode())).await()

        log.trace("Appended delta for entity {} to frame {}",
            delta.entityId, frameNumber)
    }

    /**
     * Retrieve all deltas for a specific frame
     * @param frameNumber The logical frame number
     * @return List of deltas for that frame, in order
     */
    override suspend fun getByFrame(frameNumber: Long): JsonObject? {
        val key = "frame:$frameNumber:deltas"
        val value = redisAPI.send(Command.create("JSON.GET"), key, ".").await()

        return value?.let { JsonObject(it.toString()) }
    }

    /**
     * Retrieve all deltas as a Json object
     * @return JsonObject matching the JsonRL structure
     */
    override suspend fun getAll(): JsonObject {
        val pattern = "frame:*"
        var cursor = "0"
        val allKeys = mutableListOf<String>()

        do {
            val scanResponse = redisAPI.scan(listOf(cursor, "MATCH", pattern, "COUNT", "100")).await()
            cursor = scanResponse.get(0).toString()
            val keys = scanResponse.get(1) as Response

            for (i in 0 until keys.size()) {
                allKeys.add(keys.get(i).toString())
            }
        } while (cursor != "0")

        val resultObject = JsonObject()
        coroutineScope {
            allKeys.map { key ->
                val value = redisAPI.jsonGet(listOf(key, "$")).await()
                key to JsonArray(value.toString())
            }.forEach { (key, array) ->
                resultObject.put(key, array)
            }
        }

        return resultObject
    }

    /**
     * Remove all frame deltas from memory
     */
    override suspend fun clearDeltas() {
        val pattern = "frame:*"
        var cursor = "0"
        val allKeys = mutableListOf<String>()

        do {
            val scanResponse = redisAPI.scan(listOf(cursor, "MATCH", pattern, "COUNT", "100")).await()
            cursor = scanResponse.get(0).toString()
            val keys = scanResponse.get(1) as Response

            for (i in 0 until keys.size()) {
                allKeys.add(keys.get(i).toString())
            }
        } while (cursor != "0")

        if (allKeys.isNotEmpty()) {
            redisAPI.del(allKeys).await()
        }
    }
}