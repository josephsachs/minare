package com.minare.worker

import com.minare.cache.ConnectionCache
import com.minare.core.FrameController
import com.minare.persistence.ChannelStore
import com.minare.persistence.ContextStore
import com.minare.utils.VerticleLogger
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.security.Timestamp
import java.sql.Date
import java.sql.Time
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
    private lateinit var vlog: VerticleLogger

    // Router for command socket endpoints
    private lateinit var router: Router

    // Update frame controller
    private lateinit var frameController: UpdateFrameController

    // Cache of entity to channel mappings with TTL (entity ID -> channel IDs)
    private val entityChannelCache = ConcurrentHashMap<String, Pair<Set<String>, Long>>()

    // Cache of channel to connection mappings with TTL (channel ID -> connection IDs)
    private val channelConnectionCache = ConcurrentHashMap<String, Pair<Set<String>, Long>>()

    // Accumulated updates for each connection (connection ID -> entity updates)
    private val connectionPendingUpdates = ConcurrentHashMap<String, MutableMap<String, JsonObject>>()

    // Track deployment data
    private var deployedAt: Long = 0
    private var httpServerVerticleId: String? = null
    private var useOwnHttpServer: Boolean = true
    private var httpServerPort: Int = 4226

    companion object {
        // Event bus addresses
        const val ADDRESS_ENTITY_UPDATED = "minare.entity.update"
        const val ADDRESS_CONNECTION_UPDATES = "minare.connection.updates"
        const val ADDRESS_CONNECTION_ESTABLISHED = "minare.connection.established"
        const val ADDRESS_CONNECTION_CLOSED = "minare.connection.closed"
        const val ADDRESS_INITIALIZE = "minare.update.initialize"

        // Cache TTL in milliseconds
        const val CACHE_TTL_MS = 10000L // 10 seconds

        // Default frame interval
        const val DEFAULT_FRAME_INTERVAL_MS = 100 // 10 frames per second

        const val BASE_PATH = "/update"
    }

    override suspend fun start() {
        try {
            deployedAt = System.currentTimeMillis()
            log.info("Starting UpdateVerticle at {$deployedAt}")

            vlog = VerticleLogger(this)
            vlog.logStartupStep("STARTING")
            vlog.logConfig(config)

            router = Router.router(vertx)
            vlog.logStartupStep("ROUTER_CREATED")

            // Initialize frame controller
            frameController = UpdateFrameController()
            frameController.start(DEFAULT_FRAME_INTERVAL_MS)
            log.info("Started FrameController at {${deployedAt}}")
            vlog.logStartupStep("FRAME_CONTROLLER_STARTED", mapOf(
                "frameInterval" to DEFAULT_FRAME_INTERVAL_MS
            ))

            registerEventBusConsumers()
            vlog.logStartupStep("EVENT_BUS_HANDLERS_REGISTERED")

            deploymentID?.let {
                vlog.logDeployment(it)
            }

            vlog.logStartupStep("STARTED")
            log.info("UpdateVerticle started with frame interval: {}ms", DEFAULT_FRAME_INTERVAL_MS)
        } catch (e: Exception) {
            vlog.logVerticleError("STARTUP_FAILED", e)
            log.error("Failed to start UpdateVerticle", e)
            throw e
        }
    }

    /**
     * Register all event bus consumers
     */
    private fun registerEventBusConsumers() {
        val eventBusUtils = vlog.createEventBusUtils()
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_ENTITY_UPDATED) { message, traceId ->
            handleEntityUpdate(message, traceId)
        }
        vlog.logHandlerRegistration(ADDRESS_ENTITY_UPDATED)

        // Register handler for connection established events
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_CONNECTION_ESTABLISHED) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            if (connectionId != null) {
                connectionPendingUpdates[connectionId] = ConcurrentHashMap()
                vlog.getEventLogger().trace("CONNECTION_TRACKING_INITIALIZED", mapOf(
                    "connectionId" to connectionId
                ), traceId)
            } else {
                vlog.getEventLogger().trace("CONNECTION_ID_MISSING", emptyMap(), traceId)
            }
        }
        vlog.logHandlerRegistration(ADDRESS_CONNECTION_ESTABLISHED)

        // Register handler for connection closed events
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_CONNECTION_CLOSED) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            if (connectionId != null) {
                // Clean up any pending updates for this connection
                connectionPendingUpdates.remove(connectionId)

                // Invalidate any channel cache entries containing this connection
                for ((channelId, entry) in channelConnectionCache) {
                    val (connections, _) = entry
                    if (connectionId in connections) {
                        channelConnectionCache.remove(channelId)
                    }
                }

                vlog.getEventLogger().trace("CONNECTION_TRACKING_REMOVED", mapOf(
                    "connectionId" to connectionId
                ), traceId)
            }
        }
        vlog.logHandlerRegistration(ADDRESS_CONNECTION_CLOSED)

        // Register handler for initialization
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_INITIALIZE) { message, traceId ->
            // Nothing specific needed here for now
            val reply = JsonObject().put("success", true)
                .put("status", "UpdateVerticle initialized")
            eventBusUtils.tracedReply(message, reply, traceId)
        }
        vlog.logHandlerRegistration(ADDRESS_INITIALIZE)
    }

    /**
     * Handle an entity update from the change stream.
     */
    private suspend fun handleEntityUpdate(message: Message<JsonObject>, traceId: String) {
        try {
            val entityUpdate = message.body()
            val entityId = entityUpdate.getString("_id")

            if (entityId == null) {
                vlog.getEventLogger().trace("ENTITY_UPDATE_MISSING_ID", mapOf(
                    "update" to entityUpdate.encode()
                ), traceId)
                return
            }

            // Get channels for this entity (from cache or database)
            val startTime = System.currentTimeMillis()
            val channels = getChannelsForEntity(entityId)
            val lookupTime = System.currentTimeMillis() - startTime

            if (lookupTime > 50) {
                vlog.getEventLogger().logPerformance("CHANNEL_LOOKUP", lookupTime, mapOf(
                    "entityId" to entityId,
                    "channelCount" to channels.size
                ), traceId)
            }

            if (channels.isEmpty()) {
                // No channels, no need to process further
                vlog.getEventLogger().trace("ENTITY_NO_CHANNELS", mapOf(
                    "entityId" to entityId
                ), traceId)
                return
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
                vlog.getEventLogger().trace("UPDATE_QUEUED", mapOf(
                    "entityId" to entityId,
                    "connectionCount" to processedConnections.size
                ), traceId)
            }
        } catch (e: Exception) {
            vlog.logVerticleError("ENTITY_UPDATE_PROCESSING", e, mapOf(
                "traceId" to traceId
            ))
        }
    }

    override suspend fun stop() {
        vlog.logStartupStep("STOPPING")
        frameController.stop()
        vlog.logUndeployment()
        log.info("UpdateVerticle stopped")
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
            vlog.logVerticleError("CHANNEL_LOOKUP", e, mapOf(
                "entityId" to entityId
            ))
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
            vlog.logVerticleError("CLIENT_LOOKUP", e, mapOf(
                "channelId" to channelId
            ))
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
                vlog.logVerticleError("FRAME_PROCESSING", e)
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
                vlog.logVerticlePerformance("FRAME_UPDATE", processingTime, mapOf(
                    "updatesProcessed" to totalUpdatesProcessed,
                    "connectionsProcessed" to totalConnectionsProcessed
                ))
            }
        }
    }
}