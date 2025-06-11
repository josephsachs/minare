package com.minare.worker

import com.minare.pubsub.PubSubChannelStrategy
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.client.Response
import io.vertx.redis.client.RedisOptions
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Named

/**
 * Worker verticle that subscribes to Redis pub/sub channels for entity change notifications.
 * Replaces ChangeStreamWorkerVerticle by providing fast Redis-based change notifications
 * instead of slow MongoDB change streams.
 */
class RedisPubSubWorkerVerticle @Inject constructor(
    @Named("databaseName") private val databaseName: String,
    private val pubSubChannelStrategy: PubSubChannelStrategy,
    private val vlog: VerticleLogger
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(RedisPubSubWorkerVerticle::class.java)
    private var running = false
    private var redisSubscriber: Redis? = null
    private var redisAPI: RedisAPI? = null

    companion object {
        // Same event bus addresses as ChangeStreamWorkerVerticle for compatibility
        const val ADDRESS_STREAM_STARTED = "minare.change.stream.started"
        const val ADDRESS_STREAM_STOPPED = "minare.change.stream.stopped"
        const val ADDRESS_ENTITY_UPDATED = "minare.entity.update"

        // Batching constants for performance
        const val CHANGE_BATCH_SIZE = 100
        const val MAX_WAIT_MS = 200L

        // Redis message types
        private const val MESSAGE_TYPE = "message"
        private const val PATTERN_MESSAGE_TYPE = "pmessage"
    }

    override suspend fun start() {
        try {
            running = true
            vlog.setVerticle(this)

            log.info("Starting RedisPubSubWorkerVerticle for database: {}", databaseName)

            // Initialize Redis subscriber
            initializeRedisSubscriber()

            // Start the subscription process
            launch {
                processRedisSubscriptions()
            }

            log.info("RedisPubSubWorkerVerticle started successfully")
        } catch (e: Exception) {
            log.error("Failed to start Redis pub/sub worker", e)
            throw e
        }
    }

    override suspend fun stop() {
        running = false

        try {
            redisSubscriber?.close()
            vertx.eventBus().publish(ADDRESS_STREAM_STOPPED, true)
            log.info("Redis pub/sub worker stopped")
        } catch (e: Exception) {
            log.error("Error stopping Redis pub/sub worker", e)
        }
    }

    /**
     * Initialize Redis subscriber connection
     */
    private suspend fun initializeRedisSubscriber() {
        val redisUri = System.getenv("REDIS_URI")
            ?: throw IllegalStateException("REDIS_URI environment variable is required")

        val redisOptions = RedisOptions()
            .setConnectionString(redisUri)

        redisSubscriber = Redis.createClient(vertx, redisOptions)

        log.info("Redis subscriber initialized")
    }

    /**
     * Main method to process Redis pub/sub subscriptions.
     * Determines which channels to subscribe to based on the strategy and subscribes to them.
     */
    private suspend fun processRedisSubscriptions() {
        val subscriptionJob = SupervisorJob()
        val subscriptionContext = Dispatchers.IO + subscriptionJob

        try {
            withContext(subscriptionContext) {
                // Connect and get the connection
                val connection = redisSubscriber!!.connect().await()

                // Set up message handler BEFORE subscribing
                connection.handler { response: Response ->
                    // Redis pub/sub responses come as Response objects
                    when (response.type()) {
                        io.vertx.redis.client.ResponseType.PUSH,
                        io.vertx.redis.client.ResponseType.MULTI -> {
                            // Handle pub/sub messages if response has enough elements
                            if (response.size() >= 3) {
                                processRedisResponse(response)
                            }
                        }
                        else -> {
                            // Handle other response types if needed
                        }
                    }
                }

                // Determine which Redis pub/sub channels to subscribe to
                val channelsToSubscribe = determineSubscriptionChannels()

                if (channelsToSubscribe.isEmpty()) {
                    log.warn("No Redis pub/sub channels to subscribe to")
                    return@withContext
                }

                log.info("Subscribing to Redis pub/sub channels: {}", channelsToSubscribe)

                // Subscribe to the channels using the connection
                for (channel in channelsToSubscribe) {
                    // Use pattern subscribe for wildcard channels
                    if (channel.contains("*")) {
                        connection.send(io.vertx.redis.client.Request.cmd(io.vertx.redis.client.Command.PSUBSCRIBE).arg(channel)).await()
                        log.info("Successfully subscribed to pattern: {}", channel)
                    } else {
                        connection.send(io.vertx.redis.client.Request.cmd(io.vertx.redis.client.Command.SUBSCRIBE).arg(channel)).await()
                        log.info("Successfully subscribed to channel: {}", channel)
                    }
                }

                // Publish stream started event
                vertx.eventBus().publish(ADDRESS_STREAM_STARTED, true)
                log.info("Redis pub/sub subscriptions active for {} channels", channelsToSubscribe.size)

                // Keep the subscription alive
                while (isActive && running) {
                    delay(1000) // Check every second
                }
            }
        } catch (e: Exception) {
            if (e is CancellationException) throw e
            log.error("Error in Redis subscription processing", e)

            // Try to restart after a delay if we're still running
            if (running) {
                delay(5000)
                processRedisSubscriptions() // Recursive restart
            }
        }
    }

    /**
     * Process Redis pub/sub response by extracting the message payload based on message type.
     * Handles both regular messages and pattern-matched messages, which have different formats.
     */
    private fun processRedisResponse(response: Response) {
        val messageType = response.get(0).toString()

        when (messageType) {
            MESSAGE_TYPE -> {
                // Regular message format: [message, channel, payload]
                if (response.size() >= 3) {
                    val channel = response.get(1).toString()
                    val payload = response.get(2).toString()
                    processMessagePayload(channel, payload)
                }
            }
            PATTERN_MESSAGE_TYPE -> {
                // Pattern message format: [pmessage, pattern, channel, payload]
                if (response.size() >= 4) {
                    val pattern = response.get(1).toString()
                    val channel = response.get(2).toString()
                    val payload = response.get(3).toString()
                    processMessagePayload(channel, payload)
                }
            }
        }
    }

    /**
     * Process a message payload from a specific channel.
     * Launches a coroutine to handle the message asynchronously.
     */
    private fun processMessagePayload(channel: String, payload: String) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                handleRedisMessage(payload)
            } catch (e: Exception) {
                log.error("Error handling Redis message from channel {}", channel, e)
            }
        }
    }

    /**
     * Determine which Redis pub/sub channels to subscribe to.
     * This is strategy-dependent - some strategies might subscribe to all possible channels,
     * others might use pattern subscriptions, etc.
     */
    private fun determineSubscriptionChannels(): List<String> {
        // For now, we'll use a simple approach based on the strategy type
        // More sophisticated implementations might use pattern subscriptions or dynamic discovery

        return when (pubSubChannelStrategy::class.simpleName) {
            "PerChannelPubSubStrategy" -> {
                // For per-channel strategy, we need to use pattern subscription
                // since we don't know all possible channel IDs in advance
                listOf("minare:channel:*:changes")
            }
            "GlobalPubSubStrategy" -> {
                // For global strategy, subscribe to the single global channel
                listOf("minare:entity:changes")
            }
            "ShardedPubSubStrategy" -> {
                // For sharded strategy, subscribe to all shard channels
                // This is a simplified example - real implementation might be more sophisticated
                (0..7).map { "minare:shard:${it}:changes" }
            }
            else -> {
                log.warn("Unknown pub/sub strategy: {}, using global fallback", pubSubChannelStrategy::class.simpleName)
                listOf("minare:entity:changes")
            }
        }
    }

    /**
     * Handle incoming Redis pub/sub messages
     */
    private suspend fun handleRedisMessage(message: String) {
        try {
            // Parse the Redis message
            val messageJson = JsonObject(message)

            // Extract the change notification data
            val changeNotification = extractChangeNotification(messageJson)

            if (changeNotification != null) {
                // Publish to event bus for UpdateVerticle to consume (same as MongoDB version)
                vertx.eventBus().publish(ADDRESS_ENTITY_UPDATED, changeNotification)
            }
        } catch (e: Exception) {
            log.error("Error processing Redis message: {}", message, e)
        }
    }

    /**
     * Temporary workaround for message handling using polling.
     * This is not ideal but works around Vert.x Redis client limitations.
     * In production, consider using Redis Streams or a different Redis client.
     */
    private suspend fun startMessagePolling() {
        // This is a simplified approach - in production you'd want to use Redis Streams
        // or a more sophisticated pub/sub handling mechanism
        log.warn("Using polling approach for Redis pub/sub - consider Redis Streams for production")

        while (running) {
            delay(1000) // Poll every second - not ideal but functional
            // In a real implementation, you'd use proper Redis pub/sub handling here
        }
    }

    /**
     * Extract change notification from Redis pub/sub message.
     * Redis pub/sub messages have a different format than the change notification payload.
     */
    private fun extractChangeNotification(redisMessage: JsonObject): JsonObject? {
        try {
            // Validate that this looks like a change notification
            if (redisMessage.containsKey("_id") &&
                redisMessage.containsKey("type") &&
                redisMessage.containsKey("version")) {

                return redisMessage
            }

            return null
        } catch (e: Exception) {
            log.error("Error extracting change notification from Redis message", e)
            return null
        }
    }
}