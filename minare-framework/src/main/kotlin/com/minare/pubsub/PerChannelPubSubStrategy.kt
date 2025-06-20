package com.minare.pubsub

import io.vertx.core.json.JsonObject
import javax.inject.Singleton

/**
 * Pub/sub strategy that publishes entity changes to channel-specific Redis pub/sub channels.
 *
 * For each application channel that an entity belongs to, this strategy publishes
 * to a corresponding Redis pub/sub channel using the naming convention:
 * "minare:channel:{channelId}:changes"
 *
 * This approach respects the existing channel topology that implementers have
 * set up via ChannelStore/ContextStore while providing fast Redis-based notifications.
 */
@Singleton
class PerChannelPubSubStrategy : PubSubChannelStrategy {

    companion object {
        const val CHANNEL_PREFIX = "minare:channel:"
        const val CHANNEL_SUFFIX = ":changes"
        const val CHANNEL_PATTERN = "minare:channel:*:changes"
    }

    override fun getPubSubChannels(
        entityId: String,
        entityType: String,
        entityChannelIds: List<String>
    ): List<String> {
        return entityChannelIds.map { channelId ->
            "$CHANNEL_PREFIX$channelId$CHANNEL_SUFFIX"
        }
    }

    override fun getSubscriptions(): List<PubSubChannelStrategy.SubscriptionDescriptor> {
        // For per-channel strategy, we use a pattern subscription
        // since we don't know all possible channel IDs in advance
        return listOf(
            PubSubChannelStrategy.SubscriptionDescriptor(
                channel = CHANNEL_PATTERN,
                isPattern = true,
                description = "Pattern subscription for all channel-specific change notifications"
            )
        )
    }

    override fun parseMessage(channel: String, message: String): JsonObject? {
        try {
            // Parse the message from JSON
            val messageJson = JsonObject(message)

            // Validate that it looks like a change notification
            if (messageJson.containsKey("_id") &&
                messageJson.containsKey("type") &&
                messageJson.containsKey("version")) {

                // The message is already in the format expected by down socket verticle
                return messageJson
            }

            return null
        } catch (e: Exception) {
            // Log error in real implementation
            return null
        }
    }
}