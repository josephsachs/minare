package com.minare.pubsub


/**
 * Strategy interface for determining which Redis pub/sub channels to publish entity changes to.
 * Different implementations can provide different channel topologies based on application needs.
 */
interface PubSubChannelStrategy {

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
}