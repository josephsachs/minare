package kotlin.com.minare.pubsub

import com.minare.pubsub.PubSubChannelStrategy
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

        return listOf("$SHARD_CHANNEL_PREFIX:${shard}:changes")
    }
}