package com.minare.core.storage.adapters

import com.minare.core.frames.models.FrameDelta
import com.minare.core.storage.interfaces.DeltaStore
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.RedisAPI
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

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

    override suspend fun getFrameDeltas(frameNumber: Long): List<FrameDelta> {
        val key = "frame:${frameNumber}:deltas"

        val response = redisAPI.get(key).await()

        return if (response != null && response.toString() != "null") {
            val array = JsonArray(response.toString())
            array.map { FrameDelta.fromJson(it as JsonObject) }
        } else {
            emptyList()
        }
    }

    override suspend fun getDeltasForFrameRange(startFrame: Long, endFrame: Long): List<FrameDelta> {
        val allDeltas = mutableListOf<FrameDelta>()

        for (frame in startFrame..endFrame) {
            allDeltas.addAll(getFrameDeltas(frame))
        }

        return allDeltas
    }

    override suspend fun getDeltasForEntities(entityIds: Set<String>): List<FrameDelta> {
        val keys = redisAPI.keys("frame:*:deltas").await()
        val allDeltas = mutableListOf<FrameDelta>()

        if (keys != null) {
            for (i in 0 until keys.size()) {
                val key = keys.get(i).toString()
                val frameDeltasResponse = redisAPI.lrange(key, "0", "-1").await()

                frameDeltasResponse?.forEach { element ->
                    val delta = FrameDelta.fromJson(JsonObject(element.toString()))
                    if (delta.entityId in entityIds) {
                        allDeltas.add(delta)
                    }
                }
            }
        }

        return allDeltas.sortedWith(compareBy({ it.frameNumber }, { it.version }))
    }

    override suspend fun clearAllDeltas() {
        val keys = redisAPI.keys("frame:*:deltas").await()

        if (keys != null && keys.size() > 0) {
            val keyList = (0 until keys.size()).map { keys.get(it).toString() }
            redisAPI.del(keyList).await()
            log.info("Cleared {} frame delta keys", keyList.size)
        }
    }
}