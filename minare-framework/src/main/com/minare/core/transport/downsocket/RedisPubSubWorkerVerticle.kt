package com.minare.core.transport.downsocket

import com.minare.core.transport.downsocket.pubsub.PubSubChannelStrategy
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.transport.downsocket.pubsub.UpdateBatchCoordinator
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.client.RedisOptions
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Named

/**
 * Worker verticle that subscribes to Redis pub/sub channels for entity change notifications.
 *
 * Now includes an UpdateBatchCoordinator that batches entity updates before distributing them
 * to ensure all DownSocketVerticles receive identical update batches.
 */
class RedisPubSubWorkerVerticle @Inject constructor(
    @Named("databaseName") private val databaseName: String,
    private val pubSubChannelStrategy: PubSubChannelStrategy,
    private val vlog: VerticleLogger,
    private val updateBatchCoordinator: UpdateBatchCoordinator
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(RedisPubSubWorkerVerticle::class.java)
    private var running = false
    private var redisSubscriber: Redis? = null
    private var redisAPI: RedisAPI? = null

    companion object {
        const val ADDRESS_STREAM_STARTED = "minare.change.stream.started"
        const val ADDRESS_STREAM_STOPPED = "minare.change.stream.stopped"
        const val ADDRESS_ENTITY_UPDATED = "minare.entity.update"
        const val DEFAULT_BATCH_INTERVAL_MS = 100L // 10 frames per second
    }

    override suspend fun start() {
        try {
            running = true
            vlog.setVerticle(this)

            initializeRedisSubscriber()

            updateBatchCoordinator.start(DEFAULT_BATCH_INTERVAL_MS)

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
            // Stop the batch coordinator
            updateBatchCoordinator.stop()
            log.info("UpdateBatchCoordinator stopped")

            // Close Redis connection
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
                val connection = redisSubscriber!!.connect().await()

                connection.handler { response: io.vertx.redis.client.Response ->
                    // Redis pub/sub responses come as Response objects
                    when (response.type()) {
                        io.vertx.redis.client.ResponseType.PUSH,
                        io.vertx.redis.client.ResponseType.MULTI -> {
                            if (response.size() >= 3) {
                                val messageType = response.get(0).toString()
                                if (messageType == "message") {
                                    val channel = response.get(1).toString()
                                    val messagePayload = response.get(2).toString()

                                    CoroutineScope(vertx.dispatcher()).launch {
                                        try {
                                            handleRedisMessage(messagePayload)
                                        } catch (e: Exception) {
                                            log.error("Error handling Redis message from channel {}", channel, e)
                                        }
                                    }
                                } else if (messageType == "pmessage" && response.size() >= 4) {
                                    val pattern = response.get(1).toString()
                                    val channel = response.get(2).toString()
                                    val messagePayload = response.get(3).toString()

                                    CoroutineScope(vertx.dispatcher()).launch {
                                        try {
                                            handleRedisMessage(messagePayload)
                                        } catch (e: Exception) {
                                            log.error("Error handling Redis pattern message from channel {}", channel, e)
                                        }
                                    }
                                }
                            }
                        }
                        else -> {
                            // Handle other response types if needed
                        }
                    }
                }

                val channelsToSubscribe = determineSubscriptionChannels()

                if (channelsToSubscribe.isEmpty()) {
                    log.warn("No Redis pub/sub channels to subscribe to")
                    return@withContext
                }

                log.info("Subscribing to Redis pub/sub channels: {}", channelsToSubscribe)

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

            if (running) {
                delay(5000)
                processRedisSubscriptions()
            }
        }
    }

    /**
     * Determine which Redis pub/sub channels to subscribe to.
     * Updated to use the strategy's own getSubscriptions() method instead of
     * brittle string-based class name matching. This is both type-safe and
     * delegates to the strategy itself to determine its subscription needs.
     */
    private fun determineSubscriptionChannels(): List<String> {
        return try {
            pubSubChannelStrategy.getSubscriptions().map { it.channel }

        } catch (e: Exception) {
            log.error("Error getting subscriptions from strategy: {}", pubSubChannelStrategy::class.simpleName, e)
            // Fallback to global channel in case of error
            listOf("minare:entity:changes")
        }
    }

    /**
     * Handle incoming Redis pub/sub messages.
     * Instead of publishing directly to the event bus, this now queues updates
     * for batched distribution via the UpdateBatchCoordinator.
     */
    private suspend fun handleRedisMessage(message: String) {
        log.info("RedisPubSubWorker received update: {}", message)

        try {
            val messageJson = JsonObject(message)
            val changeNotification = extractChangeNotification(messageJson)

            if (changeNotification != null) {
                updateBatchCoordinator.queueUpdate(changeNotification)

                vertx.eventBus().publish(ADDRESS_ENTITY_UPDATED, changeNotification)
            }
        } catch (e: Exception) {
            log.error("Error processing Redis message: {}", message, e)
        }
    }

    /**
     * Extract change notification from Redis pub/sub message.
     * Redis pub/sub messages have a different format than the change notification payload.
     */
    private fun extractChangeNotification(redisMessage: JsonObject): JsonObject? {
        try {
            // Redis pub/sub messages come in the format:
            // ["message", "channel_name", "actual_message_payload"]

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