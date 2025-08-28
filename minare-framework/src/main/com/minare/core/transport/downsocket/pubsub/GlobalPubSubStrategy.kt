package com.minare.core.transport.downsocket.pubsub

import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Singleton

/**
 * Pub/sub strategy that publishes all entity changes to a single global Redis channel.
 *
 * This strategy ignores the entity's specific channel memberships and publishes
 * all changes to "minare:entity:changes". This is simple and efficient for
 * applications that want to receive all entity changes in one stream.
 *
 * Use this strategy when:
 * - You have a simple application with few entities
 * - You want all changes in one stream for easier processing
 * - You don't need channel-based filtering
 */
@Singleton
class GlobalPubSubStrategy : PubSubChannelStrategy {
    private val log = LoggerFactory.getLogger(GlobalPubSubStrategy::class.java)

    companion object {
        const val GLOBAL_CHANNEL = "minare:entity:changes"
    }

    override fun getPubSubChannels(
        entityId: String,
        entityType: String,
        entityChannelIds: List<String>
    ): List<String> {
        return listOf(GLOBAL_CHANNEL)
    }

    override fun getSubscriptions(): List<PubSubChannelStrategy.SubscriptionDescriptor> {
        // For global strategy, we use a single regular subscription
        return listOf(
            PubSubChannelStrategy.SubscriptionDescriptor(
                channel = GLOBAL_CHANNEL,
                isPattern = false,
                description = "Global subscription for all entity changes"
            )
        )
    }

    override fun parseMessage(channel: String, message: String): JsonObject? {
        try {
            val messageJson = JsonObject(message)

            // Validate that it looks like a change notification
            if (messageJson.containsKey("_id") &&
                messageJson.containsKey("type") &&
                messageJson.containsKey("version")) {

                return messageJson
            }

            return null
        } catch (e: Exception) {
            log.error("PubSub message received not in expected format $e")

            return null
        }
    }
}