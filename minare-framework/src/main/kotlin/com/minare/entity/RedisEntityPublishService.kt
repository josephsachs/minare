package kotlin.com.minare.entity

import com.google.inject.Singleton
import com.minare.persistence.ContextStore
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.RedisAPI
import javax.inject.Inject

@Singleton
class RedisEntityPublishService @Inject constructor(
    private val redisAPI: RedisAPI,
    private val contextStore: ContextStore
) : EntityPublishService {

    override suspend fun publishStateChange(entityId: String, entityType: String, version: Long, delta: JsonObject) {
        // 1. Look up which channels should get this entity
        val channels = contextStore.getChannelsByEntityId(entityId)

        // 2. Build message: {entityId, type, version, delta}
        val message = JsonObject()
            .put("entityId", entityId)
            .put("type", entityType)
            .put("version", version)
            .put("delta", delta)

        // 3. Publish to each channel
        channels.forEach { channelId ->
            redisAPI.publish(channelId, message.encode()).await()
        }
    }
}