package com.minare.worker

import com.minare.pubsub.PubSubChannelStrategy
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.client.RedisOptions
import io.vertx.redis.client.ResponseType
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
    private var subscriptionJob: Job? = null

    companion object {
        // Same event bus addresses as ChangeStreamWorkerVerticle for compatibility
        const val ADDRESS_STREAM_STARTED = "minare.change.stream.started"
        const val ADDRESS_STREAM_STOPPED = "minare.change.stream.stopped"
        const val ADDRESS_ENTITY_UPDATED = "minare.entity.update"
    }

    override suspend fun start() {
        try {
            running = true
            vlog.setVerticle(this)

            log.info("Starting RedisPubSubWorkerVerticle for database: {}", databaseName)

            // Initialize Redis subscriber
            initializeRedisSubscriber()

            // Start the subscription process
            subscriptionJob = launch {
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
            // Cancel the subscription job if it's running
            subscriptionJob?.cancelAndJoin()

            // Close the Redis connection
            redisSubscriber?.close()

            // Notify that the stream has stopped
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
        val subscriptionSupervisor = SupervisorJob()
        val subscriptionContext = Dispatchers.IO + subscriptionSupervisor

        try {
            withContext(subscriptionContext) {
                // Get subscription descriptors from the strategy
                val subscriptions = pubSubChannelStrategy.getSubscriptions()

                if (subscriptions.isEmpty()) {
                    log.warn("No subscriptions defined by strategy. No Redis pub/sub monitoring will occur.")
                    return@withContext
                }

                log.info("Strategy {} provided {} subscription(s)",
                    pubSubChannelStrategy::class.simpleName, subscriptions.size)

                // Connect to Redis
                val connection = redisSubscriber!!.connect().await()

                // Set up message handler BEFORE subscribing
                connection.handler { response ->
                    // Handle Redis pub/sub messages
                    try {
                        if (response.type() == ResponseType.PUSH) {
                            val messageType = response.get(0).toString()

                            when (messageType) {
                                "message" -> {
                                    if (response.size() >= 3) {
                                        val channel = response.get(1).toString()
                                        val messagePayload = response.get(2).toString()

                                        // Process message asynchronously to avoid blocking the Redis connection
                                        CoroutineScope(vertx.dispatcher()).launch {
                                            handleRedisMessage(channel, messagePayload)
                                        }
                                    }
                                }
                                "pmessage" -> {
                                    if (response.size() >= 4) {
                                        // For pattern messages, we get pattern, channel, payload
                                        val pattern = response.get(1).toString()
                                        val channel = response.get(2).toString()
                                        val messagePayload = response.get(3).toString()

                                        // Process message asynchronously
                                        CoroutineScope(vertx.dispatcher()).launch {
                                            handleRedisMessage(channel, messagePayload)
                                        }
                                    }
                                }
                            }
                        }
                    } catch (e: Exception) {
                        log.error("Error processing Redis message", e)
                    }
                }

                // Create RedisAPI for commands
                val redisAPI = RedisAPI.api(connection)

                // Subscribe to all channels/patterns from the strategy
                for (subscription in subscriptions) {
                    try {
                        if (subscription.isPattern) {
                            log.info("Subscribing to pattern: {}", subscription.channel)
                            redisAPI.psubscribe(listOf(subscription.channel)).await()
                        } else {
                            log.info("Subscribing to channel: {}", subscription.channel)
                            redisAPI.subscribe(listOf(subscription.channel)).await()
                        }
                    } catch (e: Exception) {
                        log.error("Failed to subscribe to {}: {}",
                            if (subscription.isPattern) "pattern" else "channel",
                            subscription.channel, e)
                    }
                }

                // Notify that subscriptions are established
                vertx.eventBus().publish(ADDRESS_STREAM_STARTED, true)

                // Keep the coroutine alive while running
                while (running) {
                    delay(5000) // Just to prevent tight loop, pub/sub is handled by handler
                }
            }
        } catch (e: Exception) {
            log.error("Error in Redis pub/sub subscription process", e)

            // Try to clean up
            subscriptionSupervisor.cancel()

            // Re-throw to signal failure
            throw e
        }
    }

    /**
     * Handle incoming Redis pub/sub messages.
     * Delegates to the strategy to parse the message, then publishes it to the event bus.
     */
    private suspend fun handleRedisMessage(channel: String, message: String) {
        try {
            // Use the strategy to parse the message
            val changeNotification = pubSubChannelStrategy.parseMessage(channel, message)

            if (changeNotification != null) {
                // Publish to event bus for UpdateVerticle to consume
                vertx.eventBus().publish(ADDRESS_ENTITY_UPDATED, changeNotification)
            } else {
                log.debug("Received invalid message on channel {}, ignoring", channel)
            }
        } catch (e: Exception) {
            log.error("Error processing Redis message on channel {}: {}", channel, message, e)
        }
    }
}