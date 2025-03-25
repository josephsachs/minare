package com.minare.worker

import com.minare.cache.ConnectionCache
import com.minare.core.FrameController
import com.minare.core.websocket.UpdateSocketManager
import com.minare.persistence.ChannelStore
import com.minare.persistence.ContextStore
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.eventbus.Message
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject

/**
 * Verticle responsible for accumulating entity updates and distributing them
 * to clients on a frame-based schedule.
 *
 * This verticle hosts its own HTTP server for direct WebSocket connections.
 */
class UpdateVerticle @Inject constructor(
    private val contextStore: ContextStore,
    private val channelStore: ChannelStore,
    private val connectionCache: ConnectionCache
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(UpdateVerticle::class.java)
    private lateinit var vlog: VerticleLogger
    private lateinit var eventBusUtils: EventBusUtils

    // Router for update socket endpoints
    private lateinit var router: Router

    // HTTP server
    private var httpServer: HttpServer? = null

    // Update frame controller
    private lateinit var frameController: UpdateFrameController

    // Cache of entity to channel mappings with TTL (entity ID -> channel IDs)
    private val entityChannelCache = ConcurrentHashMap<String, Pair<Set<String>, Long>>()

    // Cache of channel to connection mappings with TTL (channel ID -> connection IDs)
    private val channelConnectionCache = ConcurrentHashMap<String, Pair<Set<String>, Long>>()

    // Accumulated updates for each connection (connection ID -> entity updates)
    private val connectionPendingUpdates = ConcurrentHashMap<String, MutableMap<String, JsonObject>>()

    // WebSocket connections for each client (connection ID -> WebSocket)
    private val updateSockets = ConcurrentHashMap<String, ServerWebSocket>()

    // Track connectionId by WebSocket
    private val socketToConnectionId = ConcurrentHashMap<ServerWebSocket, String>()

    // Track deployment data
    private var deployedAt: Long = 0
    private var useOwnHttpServer: Boolean = true
    private var httpServerPort: Int = 4226
    private var httpServerHost: String = "0.0.0.0"

    companion object {
        // Event bus addresses
        const val ADDRESS_ENTITY_UPDATED = "minare.entity.update"
        const val ADDRESS_CONNECTION_ESTABLISHED = "minare.connection.established"
        const val ADDRESS_CONNECTION_CLOSED = "minare.connection.closed"
        const val ADDRESS_UPDATE_SOCKET_INITIALIZED = "minare.update.socket.initialized"
        const val ADDRESS_UPDATE_SOCKET_CLOSE = "minare.update.socket.close"
        const val ADDRESS_INITIALIZE = "minare.update.initialize"

        // Cache TTL in milliseconds
        const val CACHE_TTL_MS = 10000L // 10 seconds

        // Default frame interval
        const val DEFAULT_FRAME_INTERVAL_MS = 100 // 10 frames per second
    }

    override suspend fun start() {
        try {
            deployedAt = System.currentTimeMillis()
            log.info("Starting UpdateVerticle at {$deployedAt}")

            vlog = VerticleLogger(this)
            eventBusUtils = vlog.createEventBusUtils()
            vlog.logStartupStep("STARTING")
            vlog.logConfig(config)

            // Get config
            useOwnHttpServer = config.getBoolean("useOwnHttpServer", true)
            httpServerPort = config.getInteger("httpPort", 4226)
            httpServerHost = config.getString("httpHost", "0.0.0.0")

            // Initialize router
            router = Router.router(vertx)
            vlog.logStartupStep("ROUTER_CREATED")

            // Initialize the router with update socket routes
            initializeRouter()

            // Initialize frame controller
            frameController = UpdateFrameController()
            frameController.start(DEFAULT_FRAME_INTERVAL_MS)
            log.info("Started FrameController at {${System.currentTimeMillis()}}")
            vlog.logStartupStep("FRAME_CONTROLLER_STARTED", mapOf(
                "frameInterval" to DEFAULT_FRAME_INTERVAL_MS
            ))

            registerEventBusConsumers()
            vlog.logStartupStep("EVENT_BUS_HANDLERS_REGISTERED")

            // Deploy HTTP server if configured to use own server
            if (useOwnHttpServer) {
                deployOwnHttpServer()
            }

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
     * Deploy a dedicated HTTP server for update sockets
     */
    private suspend fun deployOwnHttpServer() {
        vlog.logStartupStep("DEPLOYING_OWN_HTTP_SERVER")

        try {
            log.info("Starting own HTTP server on $httpServerHost:$httpServerPort")

            // Create HTTP server options
            val options = HttpServerOptions()
                .setHost(httpServerHost)
                .setPort(httpServerPort)
                .setLogActivity(true) // Enable activity logging for debugging

            // Create and start the HTTP server
            httpServer = vertx.createHttpServer(options)
                .requestHandler(router)
                .listen()
                .await()

            val actualPort = httpServer?.actualPort() ?: httpServerPort

            log.info("HTTP server started successfully on port {}", actualPort)

            vlog.logStartupStep("HTTP_SERVER_DEPLOYED", mapOf(
                "port" to actualPort,
                "host" to httpServerHost
            ))
        } catch (e: Exception) {
            vlog.logVerticleError("DEPLOY_HTTP_SERVER", e)
            log.error("Failed to deploy HTTP server", e)

            // Fall back to using the main application's HTTP server
            useOwnHttpServer = false
            throw e
        }
    }

    /**
     * Initialize the router with update socket routes
     */
    private fun initializeRouter() {
        vlog.logStartupStep("INITIALIZING_ROUTER")

        // Add a debug endpoint to check if router is working
        router.get("/debug").handler { ctx ->
            ctx.response()
                .putHeader("Content-Type", "application/json")
                .end(JsonObject()
                    .put("status", "ok")
                    .put("message", "UpdateVerticle router is working")
                    .put("timestamp", System.currentTimeMillis())
                    .encode())
        }

        // Setup main WebSocket route for updates
        log.info("Setting up update websocket route handler at path: /")

        router.route("/").handler { context ->
            val traceId = vlog.getEventLogger().trace("WEBSOCKET_ROUTE_ACCESSED", mapOf(
                "path" to "/",
                "remoteAddress" to context.request().remoteAddress().toString()
            ))

            context.request().toWebSocket()
                .onSuccess { socket ->
                    log.info("Update WebSocket upgrade successful from: {}", socket.remoteAddress())

                    launch {
                        try {
                            vlog.getEventLogger().trace("WEBSOCKET_UPGRADED", mapOf(
                                "socketId" to socket.textHandlerID()
                            ), traceId)

                            handleUpdateSocket(socket)
                        } catch (e: Exception) {
                            vlog.logVerticleError("WEBSOCKET_HANDLER", e, mapOf(
                                "socketId" to socket.textHandlerID()
                            ))

                            try {
                                if (!socket.isClosed()) {
                                    socket.close()
                                }
                            } catch (closeEx: Exception) {
                                vlog.logVerticleError("WEBSOCKET_CLOSE", closeEx, mapOf(
                                    "socketId" to socket.textHandlerID()
                                ))
                            }
                        }
                    }
                }
                .onFailure { err ->
                    log.error("WebSocket upgrade failed: {}", err.message, err)
                    vlog.logVerticleError("WEBSOCKET_UPGRADE", err)
                    context.response()
                        .setStatusCode(400)
                        .putHeader("Content-Type", "text/plain")
                        .end("Update WebSocket upgrade failed: ${err.message}")
                }
        }

        // Add health check route
        router.get("/health").handler { ctx ->
            // Get connection stats
            val connectionCount = updateSockets.size

            // Create response with detailed metrics
            val healthInfo = JsonObject()
                .put("status", "ok")
                .put("verticle", this.javaClass.simpleName)
                .put("deploymentId", deploymentID)
                .put("updateConnections", connectionCount)
                .put("timestamp", System.currentTimeMillis())
                .put("uptime", System.currentTimeMillis() - deployedAt)
                .put("metrics", JsonObject()
                    .put("pendingUpdateQueues", connectionPendingUpdates.size)
                )

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(healthInfo.encode())

            // Log health check access with low frequency
            if (Math.random() < 0.1) { // Only log ~10% of health checks to avoid log spam
                vlog.logStartupStep("HEALTH_CHECK_ACCESSED", mapOf(
                    "updateConnections" to connectionCount
                ))
            }
        }

        vlog.logStartupStep("ROUTER_INITIALIZED")
        log.info("Update socket router initialized with routes: /, /health, /debug")
    }

    /**
     * Handle a new update socket connection
     */
    private suspend fun handleUpdateSocket(websocket: ServerWebSocket) {
        val remoteAddress = websocket.remoteAddress().toString()
        log.info("New update socket connection from {}", remoteAddress)

        // Create a trace for this connection
        val socketTraceId = vlog.getEventLogger().logWebSocketEvent(
            "UPDATE_SOCKET_CONNECTED",
            null,
            mapOf(
                "remoteAddress" to remoteAddress,
                "socketId" to websocket.textHandlerID()
            )
        )

        // Set up text message handler to process the connectionId association
        websocket.textMessageHandler { message ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val msg = JsonObject(message)
                    val connectionId = msg.getString("connectionId")

                    if (connectionId != null) {
                        associateUpdateSocket(connectionId, websocket, socketTraceId)
                    } else {
                        handleError(websocket, IllegalArgumentException("No connectionId provided"))
                    }
                } catch (e: Exception) {
                    handleError(websocket, e)
                }
            }
        }

        // Set up close handler
        websocket.closeHandler {
            CoroutineScope(vertx.dispatcher()).launch {
                handleSocketClose(websocket)
            }
        }

        // Accept the websocket connection
        websocket.accept()
        vlog.getEventLogger().trace("UPDATE_WEBSOCKET_ACCEPTED", mapOf(
            "socketId" to websocket.textHandlerID()
        ), socketTraceId)
    }

    /**
     * Associate an update socket with a connection ID
     */
    private suspend fun associateUpdateSocket(connectionId: String, websocket: ServerWebSocket, traceId: String) {
        vlog.getEventLogger().trace("ASSOCIATING_SOCKET", mapOf(
            "connectionId" to connectionId,
            "socketId" to websocket.textHandlerID()
        ), traceId)

        try {
            // Verify the connection exists
            val connection = connectionCache.getConnection(connectionId)
            if (connection == null) {
                vlog.getEventLogger().trace("CONNECTION_NOT_FOUND", mapOf(
                    "connectionId" to connectionId
                ), traceId)
                handleError(websocket, IllegalArgumentException("Connection not found: $connectionId"))
                return
            }

            // Close any existing socket for this connection
            updateSockets[connectionId]?.let { existingSocket ->
                try {
                    if (!existingSocket.isClosed()) {
                        existingSocket.close()
                        vlog.getEventLogger().logWebSocketEvent("EXISTING_SOCKET_CLOSED", connectionId,
                            mapOf("socketId" to existingSocket.textHandlerID()), traceId)
                    }
                } catch (e: Exception) {
                    vlog.logVerticleError("CLOSE_EXISTING_SOCKET", e, mapOf(
                        "connectionId" to connectionId
                    ))
                }
            }

            // Store the socket
            updateSockets[connectionId] = websocket
            socketToConnectionId[websocket] = connectionId

            // Register update socket in the connection cache
            connectionCache.storeUpdateSocket(connectionId, websocket, connection)

            // Send confirmation
            sendUpdateSocketConfirmation(websocket, connectionId)

            vlog.getEventLogger().logStateChange("UpdateSocket", "CONNECTED", "REGISTERED",
                mapOf("connectionId" to connectionId), traceId)

            // Initialize pending updates for this connection if not already present
            connectionPendingUpdates.computeIfAbsent(connectionId) { ConcurrentHashMap() }

            // Notify about connection established
            vertx.eventBus().publish(ADDRESS_CONNECTION_ESTABLISHED, JsonObject()
                .put("connectionId", connectionId)
                .put("socketType", "update")
            )

            vlog.getEventLogger().trace("UPDATE_SOCKET_ASSOCIATED", mapOf(
                "connectionId" to connectionId,
                "socketId" to websocket.textHandlerID()
            ), traceId)
        } catch (e: Exception) {
            vlog.logVerticleError("ASSOCIATE_UPDATE_SOCKET", e, mapOf(
                "connectionId" to connectionId
            ))
            handleError(websocket, e)
        }
    }

    private fun sendUpdateSocketConfirmation(websocket: ServerWebSocket, connectionId: String) {
        val confirmation = JsonObject()
            .put("type", "update_socket_confirm")
            .put("connectionId", connectionId)
            .put("timestamp", System.currentTimeMillis())

        websocket.writeTextMessage(confirmation.encode())
        vlog.getEventLogger().logWebSocketEvent("UPDATE_SOCKET_CONFIRMED", connectionId,
            mapOf("timestamp" to System.currentTimeMillis()))
    }

    private suspend fun handleSocketClose(websocket: ServerWebSocket) {
        val connectionId = socketToConnectionId[websocket]
        val traceId = vlog.getEventLogger().trace("UPDATE_SOCKET_CLOSED", mapOf(
            "socketId" to websocket.textHandlerID(),
            "connectionId" to (connectionId ?: "unknown")
        ))

        try {
            if (connectionId != null) {
                // Remove from caches
                updateSockets.remove(connectionId)
                socketToConnectionId.remove(websocket)

                // Remove from connection cache
                connectionCache.removeUpdateSocket(connectionId)

                // Notify about connection closed
                vertx.eventBus().publish(ADDRESS_CONNECTION_CLOSED, JsonObject()
                    .put("connectionId", connectionId)
                    .put("socketType", "update")
                )

                vlog.getEventLogger().logStateChange("UpdateSocket", "REGISTERED", "DISCONNECTED",
                    mapOf("connectionId" to connectionId), traceId)
            } else {
                vlog.getEventLogger().trace("UNKNOWN_SOCKET_CLOSED", mapOf(
                    "socketId" to websocket.textHandlerID()
                ))
            }
        } catch (e: Exception) {
            vlog.logVerticleError("UPDATE_SOCKET_CLOSE", e, mapOf(
                "connectionId" to (connectionId ?: "unknown")
            ))
        }
    }

    private fun handleError(websocket: ServerWebSocket, error: Throwable) {
        val connectionId = socketToConnectionId[websocket]

        vlog.logVerticleError("UPDATE_SOCKET_ERROR", error, mapOf(
            "connectionId" to (connectionId ?: "unknown"),
            "remoteAddress" to websocket.remoteAddress().toString()
        ))

        try {
            val errorMessage = JsonObject()
                .put("type", "error")
                .put("message", error.message)
                .put("timestamp", System.currentTimeMillis())

            websocket.writeTextMessage(errorMessage.encode())
        } catch (e: Exception) {
            vlog.logVerticleError("ERROR_NOTIFICATION", e, mapOf(
                "connectionId" to (connectionId ?: "unknown")
            ))
        } finally {
            if (!websocket.isClosed()) {
                websocket.close()
            }
        }
    }

    /**
     * Register all event bus consumers
     */
    private fun registerEventBusConsumers() {
        // Register handler for entity updates from ChangeStreamWorkerVerticle
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_ENTITY_UPDATED) { message, traceId ->
            handleEntityUpdate(message, traceId)
        }
        vlog.logHandlerRegistration(ADDRESS_ENTITY_UPDATED)

        // Register handler for connection established events
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_CONNECTION_ESTABLISHED) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            if (connectionId != null) {
                // Only initialize if not initialized yet
                connectionPendingUpdates.computeIfAbsent(connectionId) { ConcurrentHashMap() }
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

        // Register handler for manual update socket closure
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_UPDATE_SOCKET_CLOSE) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            if (connectionId != null) {
                try {
                    val socket = updateSockets[connectionId]
                    if (socket != null && !socket.isClosed()) {
                        socket.close()
                        vlog.getEventLogger().trace("SOCKET_CLOSED_MANUALLY", mapOf(
                            "connectionId" to connectionId
                        ), traceId)
                    }
                    updateSockets.remove(connectionId)
                    eventBusUtils.tracedReply(message, JsonObject().put("success", true), traceId)
                } catch (e: Exception) {
                    vlog.logVerticleError("MANUAL_SOCKET_CLOSE", e, mapOf(
                        "connectionId" to connectionId
                    ))
                    message.fail(500, "Error closing socket: ${e.message}")
                }
            } else {
                message.fail(400, "Missing connectionId")
            }
        }
        vlog.logHandlerRegistration(ADDRESS_UPDATE_SOCKET_CLOSE)
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
     * Send an update directly to a client's WebSocket
     */
    private fun sendUpdate(connectionId: String, update: JsonObject): Boolean {
        val socket = updateSockets[connectionId]

        return if (socket != null && !socket.isClosed()) {
            try {
                socket.writeTextMessage(update.encode())
                true
            } catch (e: Exception) {
                log.error("Failed to send update to {}", connectionId, e)
                false
            }
        } else {
            log.debug("No update socket found for connection {}", connectionId)
            false
        }
    }

    override suspend fun stop() {
        vlog.logStartupStep("STOPPING")

        // Stop the frame controller
        frameController.stop()

        // Close HTTP server if we created one
        if (httpServer != null) {
            try {
                log.info("Closing HTTP server")
                httpServer!!.close().await()
                log.info("HTTP server closed successfully")
            } catch (e: Exception) {
                log.error("Error closing HTTP server", e)
            }
        }

        // Close all open WebSockets
        updateSockets.forEach { (connectionId, socket) ->
            try {
                if (!socket.isClosed()) {
                    socket.close()
                    log.debug("Closed update socket for connection: {}", connectionId)
                }
            } catch (e: Exception) {
                log.error("Error closing socket for connection: {}", connectionId, e)
            }
        }
        updateSockets.clear()
        socketToConnectionId.clear()

        vlog.logUndeployment()
        log.info("UpdateVerticle stopped")
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
                    .put("timestamp", System.currentTimeMillis())
                    .put("updates", JsonObject().apply {
                        updatesBatch.forEach { (entityId, update) ->
                            put(entityId, update)
                        }
                    })

                // Send directly to the client's WebSocket instead of using event bus
                sendUpdate(connectionId, updateMessage)
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