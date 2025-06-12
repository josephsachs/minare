package com.minare.pubsub

import io.vertx.core.json.JsonObject
import javax.inject.Singleton

/**
 * Pub/sub strategy that distributes entity changes across multiple sharded Redis channels.
 *
 * This strategy uses consistent hashing to distribute entities across a fixed number
 * of shard channels. This provides better scalability for high-volume applications
 * while allowing horizontal scaling of change stream processors.
 *
 * Use this strategy when:
 * - You have high-volume entity changes
 * - You want to scale change processing horizontally
 * - You can deploy multiple change stream workers
 * - Load distribution is more important than entity grouping
 */
@Singleton
class ShardedPubSubStrategy : PubSubChannelStrategy {

    companion object {
        const val DEFAULT_SHARD_COUNT = 8
        const val SHARD_CHANNEL_PREFIX = "minare:shard"
        const val SHARD_CHANNEL_SUFFIX = ":changes"
    }

    private val shardCount: Int = DEFAULT_SHARD_COUNT

    override fun getPubSubChannels(
        entityId: String,
        entityType: String,
        entityChannelIds: List<String>
    ): List<String> {
        // Use consistent hashing to determine shard
        val shard = entityId.hashCode().mod(shardCount).let {
            if (it < 0) it + shardCount else it
        }

        return listOf("$SHARD_CHANNEL_PREFIX:${shard}$SHARD_CHANNEL_SUFFIX")
    }

    override fun getSubscriptions(): List<PubSubChannelStrategy.SubscriptionDescriptor> {
        // For sharded strategy, we subscribe to all shard channels
        return (0 until shardCount).map { shard ->
            PubSubChannelStrategy.SubscriptionDescriptor(
                channel = "$SHARD_CHANNEL_PREFIX:${shard}$SHARD_CHANNEL_SUFFIX",
                isPattern = false,
                description = "Subscription for shard $shard"
            )
        }
    }

    override fun parseMessage(channel: String, message: String): JsonObject? {
        try {
            // Parse the message from JSON
            val messageJson = JsonObject(message)

            // Validate that it looks like a change notification
            if (messageJson.containsKey("_id") &&
                messageJson.containsKey("type") &&
                messageJson.containsKey("version")) {

                // The message is already in the format expected by update verticle
                return messageJson
            }

            return null
        } catch (e: Exception) {
            // Log error in real implementation
            return null
        }
    }
}