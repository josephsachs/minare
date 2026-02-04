package com.minare.core.transport.downsocket

import com.minare.application.config.FrameworkConfig
import com.minare.core.transport.downsocket.pubsub.PubSubChannelStrategy
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.transport.downsocket.pubsub.UpdateBatchCoordinator
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.redis.client.Redis
import io.vertx.redis.client.RedisAPI
import io.vertx.redis.client.RedisOptions
import io.vertx.redis.client.Response
import io.vertx.redis.client.Request
import io.vertx.redis.client.ResponseType
import io.vertx.redis.client.Command
import kotlinx.coroutines.*
import org.slf4j.LoggerFactory
import com.google.inject.Inject

/**
 * Worker verticle that subscribes to Redis pub/sub channels for entity change notifications.
 *
 * Now includes an UpdateBatchCoordinator that batches entity updates before distributing them
 * to ensure all DownSocketVerticles receive identical update batches.
 */
class RedisPubSubWorkerVerticle @Inject constructor(
    private val frameworkConfig: FrameworkConfig,
    private val pubSubChannelStrategy: PubSubChannelStrategy,
    private val vlog: VerticleLogger,
    private val updateBatchCoordinator: UpdateBatchCoordinator,
    private val debug: DebugLogger
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(RedisPubSubWorkerVerticle::class.java)
    private var running = false
    private var redisSubscriber: Redis? = null
    private var redisAPI: RedisAPI? = null
    private var subscriptionStopper = CompletableDeferred<Unit>()

    companion object {
        const val ADDRESS_STREAM_STARTED = "minare.change.stream.started"
        const val ADDRESS_STREAM_STOPPED = "minare.change.stream.stopped"
        const val ADDRESS_ENTITY_UPDATED = "minare.entity.update"
    }

    override suspend fun start() {
        try {
            running = true
            subscriptionStopper = CompletableDeferred()
            vlog.setVerticle(this)

            initializeRedisSubscriber()

            if (frameworkConfig.entity.update.collectChanges) {
                updateBatchCoordinator.start(frameworkConfig.entity.update.interval)
            }

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
        subscriptionStopper.complete(Unit)

        try {
            if (frameworkConfig.entity.update.collectChanges) {
                updateBatchCoordinator.stop()
                log.info("UpdateBatchCoordinator stopped")
            }

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
        val redisOptions = RedisOptions()
            .setConnectionString(
                "redis://${frameworkConfig.redis.host}:${frameworkConfig.redis.port}"
            )
        redisSubscriber = Redis.createClient(vertx, redisOptions)
    }

    private fun handleMessage(payload: String, channel: String) {
        CoroutineScope(vertx.dispatcher()).launch {
            try {
                handleRedisMessage(payload)
            } catch (e: Exception) {
                log.error("Error handling Redis message from channel {}", channel, e)
            }
        }
    }

    private fun isResponseWellFormed(response: Response): Boolean {
        return response.size() >= 3
    }

    private fun isMessage(response: Response): Boolean {
        return response.get(0).toString() == "message"
    }

    private fun isPatternMessage(response: Response): Boolean {
        return response.get(0).toString() == "pmessage" && response.size() >= 4
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

                connection.handler { response: Response ->
                    // Redis pub/sub responses come as Response objects
                    when (response.type()) {
                        ResponseType.PUSH,
                        ResponseType.MULTI -> {
                            if (isResponseWellFormed(response)) {
                                if (isMessage(response)) {
                                    val channel = response.get(1).toString()
                                    val messagePayload = response.get(2).toString()

                                    handleMessage(messagePayload, channel)
                                } else if (isPatternMessage(response)) {
                                    val pattern = response.get(1).toString()
                                    val channel = response.get(2).toString()
                                    val messagePayload = response.get(3).toString()

                                    handleMessage(messagePayload, channel)
                                }
                            }
                        }
                        else -> {
                            // Handle other response types if needed
                        }
                    }
                }

                subscribeToChannels(connection)
                vertx.eventBus().publish(ADDRESS_STREAM_STARTED, true)

                subscriptionStopper.await()
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

    private suspend fun subscribeToChannels(connection: io.vertx.redis.client.RedisConnection) {
        val channelsToSubscribe = determineSubscriptionChannels()

        if (channelsToSubscribe.isEmpty()) {
            debug.log(DebugType.DOWNSOCKET_PUBSUB_WORKER_NO_CHANNELS)
            return
        }

        for (channel in channelsToSubscribe) {
            // Use pattern subscribe for wildcard channels
            if (channel.contains("*")) {
                connection.send(Request.cmd(Command.PSUBSCRIBE).arg(channel)).await()
                debug.log(DebugType.DOWNSOCKET_PUBSUB_WORKER_SUBSCRIBED_TO_PATTERN, listOf(channel))
            } else {
                connection.send(Request.cmd(Command.SUBSCRIBE).arg(channel)).await()
                debug.log(DebugType.DOWNSOCKET_PUBSUB_WORKER_SUBSCRIBED_TO_CHANNEL, listOf(channel))
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
     * Routes updates based on configuration:
     * - If collectChanges is false: publish directly (no batching/deduplication)
     * - If collectChanges is true with interval=0: queue and flush immediately (deduplication only)
     * - If collectChanges is true with interval>0: queue for timed batch distribution
     */
    private suspend fun handleRedisMessage(message: String) {
        debug.log(DebugType.DOWNSOCKET_PUBSUB_WORKER_RECEIVED_UPDATE, listOf(message))

        try {
            val messageJson = JsonObject(message)
            val changeNotification = extractChangeNotification(messageJson)

            if (changeNotification != null) {
                if (frameworkConfig.entity.update.collectChanges) {
                    updateBatchCoordinator.queueUpdate(changeNotification)

                    if (frameworkConfig.entity.update.interval == 0L) {
                        updateBatchCoordinator.flushBatch()
                    }
                } else {
                    publishUpdateDirectly(changeNotification)
                }

                vertx.eventBus().publish(ADDRESS_ENTITY_UPDATED, changeNotification)
            }
        } catch (e: Exception) {
            log.error("Error processing Redis message: {}", message, e)
        }
    }

    /**
     * Publish update directly without batching or deduplication.
     * Used when collectChanges is disabled.
     */
    private fun publishUpdateDirectly(changeNotification: JsonObject) {
        val updateMessage = JsonObject()
            .put("type", "update_batch")
            .put("timestamp", System.currentTimeMillis())
            .put("updates", JsonObject().put(changeNotification.getString("_id"), changeNotification))

        vertx.eventBus().publish(UpdateBatchCoordinator.ADDRESS_BATCHED_UPDATES, updateMessage)
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