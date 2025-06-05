package com.minare.pubsub

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
}