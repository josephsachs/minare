package com.minare.pubsub

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

    override fun getPubSubChannels(
        entityId: String,
        entityType: String,
        entityChannelIds: List<String>
    ): List<String> {
        return entityChannelIds.map { channelId ->
            "minare:channel:${channelId}:changes"
        }
    }
}