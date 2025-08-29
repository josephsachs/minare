package com.minare.entity

import com.google.inject.Singleton
import com.minare.persistence.ContextStore
import com.minare.pubsub.PubSubChannelStrategy
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.RedisAPI
import javax.inject.Inject

@Singleton
class RedisEntityPublishService @Inject constructor(
    private val redisAPI: RedisAPI,
    private val contextStore: ContextStore,
    private val pubSubChannelStrategy: PubSubChannelStrategy
) : EntityPublishService {

    override suspend fun publishStateChange(entityId: String, entityType: String, version: Long, delta: JsonObject) {
        // 1. Look up which application channels should get this entity (existing behavior)
        val channels = contextStore.getChannelsByEntityId(entityId)

        // 2. Build message: {entityId, type, version, delta} (existing behavior)
        val message = JsonObject()
            .put("entityId", entityId)
            .put("type", entityType)
            .put("version", version)
            .put("delta", delta)

        // 3. Publish to each application channel (existing behavior)
        channels.forEach { channelId ->
            redisAPI.publish(channelId, message.encode()).await()
        }

        // 4. NEW: Publish change notification to Redis pub/sub channels for fast change stream processing
        publishToPubSubChannels(entityId, entityType, version, delta, channels)
    }

    /**
     * Publish entity change notification to Redis pub/sub channels for change stream processing.
     * This provides fast notifications that replace MongoDB change streams.
     */
    private suspend fun publishToPubSubChannels(
        entityId: String,
        entityType: String,
        version: Long,
        delta: JsonObject,
        entityChannelIds: List<String>
    ) {
        try {
            // Get the Redis pub/sub channels to publish to
            val pubSubChannels = pubSubChannelStrategy.getPubSubChannels(entityId, entityType, entityChannelIds)

            if (pubSubChannels.isEmpty()) {
                return
            }

            // Create change notification message matching MongoDB change stream format
            val changeNotification = JsonObject()
                .put("type", entityType)
                .put("_id", entityId)
                .put("version", version)
                .put("operation", "update")
                .put("changedAt", System.currentTimeMillis())
                .put("delta", delta)

            // Publish to each Redis pub/sub channel
            pubSubChannels.forEach { channel ->
                redisAPI.publish(channel, changeNotification.encode()).await()
            }
        } catch (e: Exception) {
            // Log error but don't fail the publication - pub/sub notifications are not critical for data consistency
            // TODO: Add proper logging when logger is available
        }
    }
}