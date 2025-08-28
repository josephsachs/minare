package com.minare.core.transport.downsocket.pubsub

import io.vertx.core.json.JsonObject

/**
 * Strategy interface for determining which Redis pub/sub channels to publish entity changes to
 * and how to interpret messages from those channels.
 * Different implementations can provide different channel topologies based on application needs.
 */
interface PubSubChannelStrategy {

    /**
     * Represents a subscription to a Redis pub/sub channel or pattern
     * Encapsulates how to subscribe and interpret messages
     */
    data class SubscriptionDescriptor(
        val channel: String,
        val isPattern: Boolean,
        val description: String
    )

    /**
     * Get the list of Redis pub/sub channels that entity changes should be published to.
     *
     * @param entityId The ID of the entity that changed
     * @param entityType The type of the entity (e.g., "Node", "User", etc.)
     * @param entityChannelIds The list of application channels this entity belongs to
     *                        (from ContextStore.getChannelsByEntityId)
     * @return List of Redis pub/sub channel names to publish the change notification to
     */
    fun getPubSubChannels(
        entityId: String,
        entityType: String,
        entityChannelIds: List<String>
    ): List<String>

    /**
     * Get the list of subscriptions that this strategy requires.
     * This allows the strategy to determine both which channels to subscribe to
     * and how to subscribe to them (regular vs pattern).
     *
     * @return List of subscription descriptors
     */
    fun getSubscriptions(): List<SubscriptionDescriptor>

    /**
     * Parse a Redis pub/sub message from a specific channel.
     * Extracts the entity change notification from the raw Redis message.
     *
     * @param channel The channel the message was received on
     * @param message The raw message payload as a string
     * @return The extracted change notification, or null if the message isn't valid
     */
    fun parseMessage(channel: String, message: String): JsonObject?
}