package com.minare.core.entity.services

import com.google.inject.Singleton
import com.minare.core.storage.interfaces.ContextStore
import com.minare.core.transport.downsocket.pubsub.PubSubChannelStrategy
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.RedisAPI
import com.google.inject.Inject

@Singleton
class RedisEntityPublishService @Inject constructor(
    private val redisAPI: RedisAPI,
    private val contextStore: ContextStore,
    private val pubSubChannelStrategy: PubSubChannelStrategy
) : EntityPublishService {
    private val debug = DebugLogger()

    override suspend fun publishStateChange(entityId: String, entityType: String, version: Long, delta: JsonObject) {
        val channels = contextStore.getChannelsByEntityId(entityId)

        val message = JsonObject()
            .put("entityId", entityId)
            .put("type", entityType)
            .put("version", version)
            .put("delta", delta)

        channels.forEach { channelId ->
            redisAPI.publish(channelId, message.encode()).await()
        }

        publishToPubSubChannels(entityId, entityType, version, delta, channels)
    }

    /**
     * Publish entity change notification to Redis pub/sub channels for change stream processing.
     */
    private suspend fun publishToPubSubChannels(
        entityId: String,
        entityType: String,
        version: Long,
        delta: JsonObject,
        entityChannelIds: List<String>
    ) {
        try {
            val pubSubChannels = pubSubChannelStrategy.getPubSubChannels(entityId, entityType, entityChannelIds)

            if (pubSubChannels.isEmpty()) {
                return
            }

            val changeNotification = JsonObject()
                .put("type", entityType)
                .put("_id", entityId)
                .put("version", version)
                .put("operation", "update")
                .put("changedAt", System.currentTimeMillis())
                .put("delta", delta)

            pubSubChannels.forEach { channel ->
                redisAPI.publish(channel, changeNotification.encode()).await()
            }
        } catch (e: Exception) {
            debug.log(DebugType.DOWNSOCKET_ENTITY_PUBLISHER_FAILED, listOf(e))
        }
    }
}