package com.minare.worker

import com.minare.cache.ConnectionCache
import com.minare.core.FrameController
import com.minare.persistence.ChannelStore
import com.minare.persistence.ContextStore
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject

/**
 * Verticle responsible for accumulating entity updates and distributing them
 * to clients on a frame-based schedule.
 */
class UpdateVerticle @Inject constructor(
    private val contextStore: ContextStore,
    private val channelStore: ChannelStore,
    private val connectionCache: ConnectionCache
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(UpdateVerticle::class.java)

    // Update frame controller
    private lateinit var frameController: UpdateFrameController

    // Cache of entity to channel mappings with TTL (entity ID -> channel IDs)
    private val entityChannelCache = ConcurrentHashMap<String, Pair<Set<String>, Long>>()

    // Cache of channel to connection mappings with TTL (channel ID -> connection IDs)
    private val channelConnectionCache = ConcurrentHashMap<String, Pair<Set<String>, Long>>()

    // Accumulated updates for each connection (connection ID -> entity updates)
    private val connectionPendingUpdates = ConcurrentHashMap<String, MutableMap<String, JsonObject>>()

    companion object {
        // Event bus addresses
        const val ADDRESS_ENTITY_UPDATED = "minare.entity.update"
        const val ADDRESS_CONNECTION_UPDATES = "minare.connection.updates"
        const val ADDRESS_CONNECTION_ESTABLISHED = "minare.connection.established"
        const val ADDRESS_CONNECTION_CLOSED = "minare.connection.closed"

        // Cache TTL in milliseconds
        const val CACHE_TTL_MS = 10000L // 10 seconds

        // Default frame interval
        const val DEFAULT_FRAME_INTERVAL_MS = 100 // 10 frames per second
    }

    override suspend fun start() {
        try {
            // Initialize frame controller
            frameController = UpdateFrameController()
            frameController.start(DEFAULT_FRAME_INTERVAL_MS)

            // Register event bus consumers
            vertx.eventBus().consumer<JsonObject>(ADDRESS_ENTITY_UPDATED, this::handleEntityUpdate)
            vertx.eventBus().consumer<JsonObject>(ADDRESS_CONNECTION_ESTABLISHED, this::handleConnectionEstablished)
            vertx.eventBus().consumer<JsonObject>(ADDRESS_CONNECTION_CLOSED, this::handleConnectionClosed)

            log.info("UpdateVerticle started with frame interval: {}ms", DEFAULT_FRAME_INTERVAL_MS)
        } catch (e: Exception) {
            log.error("Failed to start UpdateVerticle", e)
            throw e
        }
    }

    override suspend fun stop() {
        frameController.stop()
        log.info("UpdateVerticle stopped")
    }

    /**
     * Handle an entity update from the change stream.
     */
    private fun handleEntityUpdate(message: Message<JsonObject>) {
        launch(vertx.dispatcher()) {
            try {
                val entityUpdate = message.body()
                val entityId = entityUpdate.getString("_id")

                if (entityId == null) {
                    log.warn("Received entity update without ID: {}", entityUpdate.encode())
                    return@launch
                }

                // Get channels for this entity (from cache or database)
                val channels = getChannelsForEntity(entityId)

                if (channels.isEmpty()) {
                    // No channels, no need to process further
                    return@launch
                }

                // For each channel, get all connections and queue the update
                val processedConnections = mutableSetOf<String>()

                for (channelId in channels) {
                    val connections = getConnectionsForChannel(channelId)

                    for (connectionId in connections) {
                        // Avoid processing the same connection multiple times
                        if (connectionId in processedConnections) {
                            continue
                        }

                        processedConnections.add(connectionId)

                        // Queue update for this connection
                        queueUpdateForConnection(connectionId, entityId, entityUpdate)
                    }
                }

                if (processedConnections.isNotEmpty()) {
                    log.debug("Queued update for entity {} to {} connections",
                        entityId, processedConnections.size)
                }
            } catch (e: Exception) {
                log.error("Error handling entity update: {}", e.message, e)
            }
        }
    }

    /**
     * Handle a new connection being established.
     */
    private fun handleConnectionEstablished(message: Message<JsonObject>) {
        val connectionInfo = message.body()
        val connectionId = connectionInfo.getString("connectionId")

        if (connectionId == null) {
            log.warn("Received connection established event without connection ID")
            return
        }

        // Initialize empty update queue for this connection
        connectionPendingUpdates[connectionId] = ConcurrentHashMap()

        log.info("Initialized update tracking for new connection: {}", connectionId)
    }

    /**
     * Handle a connection being closed.
     */
    private fun handleConnectionClosed(message: Message<JsonObject>) {
        val connectionInfo = message.body()
        val connectionId = connectionInfo.getString("connectionId")

        if (connectionId == null) {
            log.warn("Received connection closed event without connection ID")
            return
        }

        // Clean up any pending updates for this connection
        connectionPendingUpdates.remove(connectionId)

        // Invalidate any channel cache entries containing this connection
        for ((channelId, entry) in channelConnectionCache) {
            val (connections, _) = entry
            if (connectionId in connections) {
                channelConnectionCache.remove(channelId)
            }
        }

        log.info("Cleaned up update tracking for closed connection: {}", connectionId)
    }

    /**
     * Queue an entity update for a specific connection.
     * If the entity already has a pending update, it will be replaced with the newer version.
     */
    private fun queueUpdateForConnection(connectionId: String, entityId: String, entityUpdate: JsonObject) {
        val updates = connectionPendingUpdates.computeIfAbsent(connectionId) { ConcurrentHashMap() }

        // Check if we already have an update for this entity
        val existingUpdate = updates[entityId]

        if (existingUpdate != null) {
            // Compare versions and only replace if newer
            val existingVersion = existingUpdate.getLong("version", 0)
            val newVersion = entityUpdate.getLong("version", 0)

            if (newVersion > existingVersion) {
                // New version is higher, replace the pending update
                updates[entityId] = entityUpdate
            }
        } else {
            // No existing update, add this one
            updates[entityId] = entityUpdate
        }
    }

    /**
     * Get all channels that contain a specific entity.
     * Uses a cache with TTL to reduce database queries.
     */
    private suspend fun getChannelsForEntity(entityId: String): Set<String> {
        val now = System.currentTimeMillis()

        // Check cache first
        val cachedEntry = entityChannelCache[entityId]
        if (cachedEntry != null) {
            val (channels, expiry) = cachedEntry
            if (now < expiry) {
                // Cache entry is still valid
                return channels
            }
            // Cache entry expired, remove it
            entityChannelCache.remove(entityId)
        }

        // Query database
        val channels = try {
            contextStore.getChannelsByEntityId(entityId).toSet()
        } catch (e: Exception) {
            log.error("Error fetching channels for entity {}: {}", entityId, e.message, e)
            emptySet()
        }

        // Cache the result with expiry
        entityChannelCache[entityId] = Pair(channels, now + CACHE_TTL_MS)

        return channels
    }

    /**
     * Get all connections subscribed to a specific channel.
     * Uses a cache with TTL to reduce database queries.
     */
    private suspend fun getConnectionsForChannel(channelId: String): Set<String> {
        val now = System.currentTimeMillis()

        // Check cache first
        val cachedEntry = channelConnectionCache[channelId]
        if (cachedEntry != null) {
            val (connections, expiry) = cachedEntry
            if (now < expiry) {
                // Cache entry is still valid
                return connections
            }
            // Cache entry expired, remove it
            channelConnectionCache.remove(channelId)
        }

        // Query database
        val connections = try {
            channelStore.getClientIds(channelId).toSet()
        } catch (e: Exception) {
            log.error("Error fetching clients for channel {}: {}", channelId, e.message, e)
            emptySet()
        }

        // Cache the result with expiry
        channelConnectionCache[channelId] = Pair(connections, now + CACHE_TTL_MS)

        return connections
    }

    /**
     * Frame controller implementation for update processing.
     */
    private inner class UpdateFrameController : FrameController(vertx) {
        override fun tick() {
            try {
                processAndSendUpdates()
            } catch (e: Exception) {
                log.error("Error in update frame processing: {}", e.message, e)
            }
        }

        /**
         * Process and send all pending updates.
         */
        private fun processAndSendUpdates() {
            val startTime = System.currentTimeMillis()
            var totalConnectionsProcessed = 0
            var totalUpdatesProcessed = 0

            // Iterate through all connections with pending updates
            for ((connectionId, updates) in connectionPendingUpdates) {
                if (updates.isEmpty()) {
                    continue
                }

                // Create a copy of the current updates and clear the pending queue
                val updatesBatch = HashMap(updates)
                updates.clear()

                if (updatesBatch.isEmpty()) {
                    continue
                }

                totalConnectionsProcessed++
                totalUpdatesProcessed += updatesBatch.size

                // Create the update message for this connection
                val updateMessage = JsonObject()
                    .put("type", "update_batch")
                    .put("connectionId", connectionId)
                    .put("timestamp", System.currentTimeMillis())
                    .put("updates", JsonObject().apply {
                        updatesBatch.forEach { (entityId, update) ->
                            put(entityId, update)
                        }
                    })

                // Send the update over the event bus
                vertx.eventBus().publish("$ADDRESS_CONNECTION_UPDATES.$connectionId", updateMessage)
            }

            val processingTime = System.currentTimeMillis() - startTime

            // Log processing stats periodically or if significant work was done
            if (totalUpdatesProcessed > 0 && (
                        totalUpdatesProcessed > 100 ||
                                processingTime > getFrameIntervalMs() / 2 ||
                                frameCount % 100 == 0L
                        )) {
                log.info("Frame update: processed {} updates for {} connections in {}ms",
                    totalUpdatesProcessed, totalConnectionsProcessed, processingTime)
            }
        }
    }
}