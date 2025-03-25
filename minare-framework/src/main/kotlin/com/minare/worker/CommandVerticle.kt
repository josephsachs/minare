package com.minare.worker

import com.minare.cache.ConnectionCache
import com.minare.controller.ConnectionController
import com.minare.core.websocket.CommandMessageHandler
import com.minare.persistence.ConnectionStore
import com.minare.persistence.ChannelStore
import com.minare.persistence.EntityStore
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import com.minare.utils.HeartbeatManager
import com.minare.utils.ConnectionTracker
import com.minare.utils.HttpServerUtils
import com.minare.utils.WebSocketUtils
import io.vertx.core.http.HttpServer
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * Verticle responsible for managing command socket connections and handling
 * socket lifecycle events. Creates and manages its own router for WebSocket endpoints.
 */
class CommandVerticle @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache,
    private val connectionController: ConnectionController,
    private val channelStore: ChannelStore,
    private val messageHandler: CommandMessageHandler,
    private val entityStore: EntityStore
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(CommandVerticle::class.java)
    private lateinit var vlog: VerticleLogger
    private lateinit var eventBusUtils: EventBusUtils

    // Core utility managers
    private lateinit var heartbeatManager: HeartbeatManager
    private lateinit var connectionTracker: ConnectionTracker

    // Router for command socket endpoints
    private lateinit var router: Router

    // HTTP server if we're using our own
    private var httpServer: HttpServer? = null

    // Track deployment data
    private var deployedAt: Long = 0
    private var useOwnHttpServer: Boolean = false
    private var httpServerPort: Int = 4225
    private var httpServerHost: String = "0.0.0.0"

    companion object {
        const val ADDRESS_COMMAND_SOCKET_INITIALIZE = "minare.command.socket.initialize"
        const val ADDRESS_COMMAND_SOCKET_HANDLE = "minare.command.socket.handle"
        const val ADDRESS_COMMAND_SOCKET_CLOSE = "minare.command.socket.close"
        const val ADDRESS_CONNECTION_CLEANUP = "minare.connection.cleanup"
        const val ADDRESS_CHANNEL_CLEANUP = "minare.channel.cleanup"
        const val ADDRESS_SOCKET_CLEANUP = "minare.socket.cleanup"
        const val ADDRESS_ENTITY_SYNC = "minare.entity.sync"
        const val ADDRESS_REGISTER_WEBSOCKET_HANDLER = "minare.register.websocket.handler"
        const val ADDRESS_GET_ROUTER = "minare.command.socket.get.router"

        // Extended handshake timeout from 500ms to 3000ms (3 seconds)
        const val HANDSHAKE_TIMEOUT_MS = 3000L
        const val HEARTBEAT_INTERVAL_MS = 15000L

        // Base path for command socket routes
        const val BASE_PATH = "/command"
    }

    override suspend fun start() {
        deployedAt = System.currentTimeMillis()
        log.info("Starting CommandSocketVerticle at {$deployedAt}")

        // Initialize logging and utility managers
        vlog = VerticleLogger(this)
        eventBusUtils = vlog.createEventBusUtils()
        connectionTracker = ConnectionTracker("CommandSocket", vlog)
        heartbeatManager = HeartbeatManager(vertx, vlog, connectionStore, CoroutineScope(vertx.dispatcher()))
        heartbeatManager.setHeartbeatInterval(HEARTBEAT_INTERVAL_MS)

        vlog.logStartupStep("STARTING")
        vlog.logConfig(config)

        // Get configuration
        useOwnHttpServer = config.getBoolean("useOwnHttpServer", true)
        httpServerPort = config.getInteger("httpPort", 4225)
        httpServerHost = config.getString("httpHost", "0.0.0.0")

        // Initialize the router
        router = Router.router(vertx)
        vlog.logStartupStep("ROUTER_CREATED")

        // Initialize the router with command socket routes
        initializeRouter()

        registerEventBusConsumers()
        vlog.logStartupStep("EVENT_BUS_HANDLERS_REGISTERED")

        // Deploy HTTP server if configured to use own server
        if (useOwnHttpServer) {
            deployOwnHttpServer()
        }

        // Save deployment ID
        deploymentID?.let {
            vlog.logDeployment(it)
        }

        vlog.logStartupStep("STARTED")
    }

    /**
     * Initialize the router with command socket routes
     */
    private fun initializeRouter() {
        vlog.logStartupStep("INITIALIZING_ROUTER")

        // Add a debug endpoint to check if router is working
        HttpServerUtils.addDebugEndpoint(router, "/ws-debug", "CommandSocketVerticle")

        // Setup main WebSocket route for commands
        log.info("Setting up websocket route handler at path: {}", BASE_PATH)

        router.route("$BASE_PATH").handler { context ->
            WebSocketUtils.handleWebSocketUpgrade(
                context,
                vertx.dispatcher(),
                BASE_PATH,
                vlog
            ) { socket, traceId ->
                handleCommandSocket(socket, traceId)
            }
        }

        // Add health check route
        HttpServerUtils.addHealthEndpoint(
            router = router,
            path = "$BASE_PATH/health",
            verticleName = "CommandSocketVerticle",
            deploymentId = deploymentID,
            deployedAt = deployedAt
        ) {
            JsonObject()
                .put("connections", connectionTracker.getMetrics())
                .put("heartbeats", heartbeatManager.getMetrics())
        }

        vlog.logStartupStep("ROUTER_INITIALIZED")
        log.info("Command socket router initialized with routes: $BASE_PATH, $BASE_PATH/health, /ws-debug")
    }

    /**
     * Deploy a dedicated HTTP server for command sockets
     */
    private suspend fun deployOwnHttpServer() {
        vlog.logStartupStep("DEPLOYING_OWN_HTTP_SERVER")

        try {
            // Use the utility to create and start the server
            httpServer = HttpServerUtils.createAndStartHttpServer(
                vertx = vertx,
                router = router,
                host = httpServerHost,
                port = httpServerPort
            ).await()

            val actualPort = httpServer?.actualPort() ?: httpServerPort
            vlog.logStartupStep(
                "HTTP_SERVER_DEPLOYED", mapOf(
                    "port" to actualPort,
                    "host" to httpServerHost
                )
            )
        } catch (e: Exception) {
            vlog.logVerticleError("DEPLOY_HTTP_SERVER", e)
            log.error("Failed to deploy HTTP server", e)

            // Fall back to using the main application's HTTP server
            useOwnHttpServer = false
            throw e
        }
    }

    /**
     * Register all event bus consumers
     */
    private fun registerEventBusConsumers() {
        // Register for initialize command
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_COMMAND_SOCKET_INITIALIZE) { message, traceId ->
            try {
                vlog.logStartupStep("INITIALIZING_ROUTER", mapOf("traceId" to traceId))

                val startTime = System.currentTimeMillis()
                // Router is already initialized in start() method
                val initTime = System.currentTimeMillis() - startTime

                vlog.logVerticlePerformance("ROUTER_INITIALIZATION", initTime)

                if (useOwnHttpServer) {
                    deployOwnHttpServer()
                }

                val reply = JsonObject()
                    .put("success", true)
                    .put(
                        "message", "Command socket router initialized" +
                                (if (useOwnHttpServer) " with dedicated HTTP server on port $httpServerPort" else "")
                    )

                eventBusUtils.tracedReply(message, reply, traceId)

                vlog.logStartupStep(
                    "ROUTER_INITIALIZED", mapOf(
                        "status" to "success",
                        "initTime" to initTime,
                        "useOwnHttpServer" to useOwnHttpServer
                    )
                )
            } catch (e: Exception) {
                vlog.logVerticleError("INITIALIZE_ROUTER", e)
                message.fail(500, "Failed to initialize router: ${e.message}")
            }
        }

        // Register event bus handler for getting the router
        vertx.eventBus().consumer<JsonObject>(ADDRESS_GET_ROUTER) { message ->
            // Reply with the router ID from shared data
            message.reply(JsonObject().put("routerId", router.toString()))
        }

        // Register event bus handler for entity sync
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_ENTITY_SYNC) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            val entityId = message.body().getString("entityId")

            try {
                vlog.logStartupStep(
                    "ENTITY_SYNC_REQUEST", mapOf(
                        "entityId" to entityId,
                        "connectionId" to connectionId,
                        "traceId" to traceId
                    )
                )

                val result = handleEntitySync(connectionId, entityId)

                eventBusUtils.tracedReply(message, JsonObject().put("success", result), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError(
                    "ENTITY_SYNC", e, mapOf(
                        "entityId" to entityId,
                        "connectionId" to connectionId
                    )
                )
                message.fail(500, e.message ?: "Error handling entity sync")
            }
        }

        // Register cleanup consumers
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_CONNECTION_CLEANUP) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            vlog.logStartupStep("CONNECTION_CLEANUP_REQUEST", mapOf("connectionId" to connectionId))

            try {
                val result = cleanupConnection(connectionId)
                eventBusUtils.tracedReply(message, JsonObject().put("success", result), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError("CONNECTION_CLEANUP", e, mapOf("connectionId" to connectionId))
                message.fail(500, e.message ?: "Error during connection cleanup")
            }
        }

        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_CHANNEL_CLEANUP) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            vlog.logStartupStep("CHANNEL_CLEANUP_REQUEST", mapOf("connectionId" to connectionId))

            try {
                val result = cleanupConnectionChannels(connectionId)
                eventBusUtils.tracedReply(message, JsonObject().put("success", result), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError("CHANNEL_CLEANUP", e, mapOf("connectionId" to connectionId))
                message.fail(500, e.message ?: "Error during channel cleanup")
            }
        }

        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_SOCKET_CLEANUP) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            val hasUpdateSocket = message.body().getBoolean("hasUpdateSocket", false)
            vlog.logStartupStep(
                "SOCKET_CLEANUP_REQUEST", mapOf(
                    "connectionId" to connectionId,
                    "hasUpdateSocket" to hasUpdateSocket
                )
            )

            try {
                val result = cleanupConnectionSockets(connectionId, hasUpdateSocket)
                eventBusUtils.tracedReply(message, JsonObject().put("success", result), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError("SOCKET_CLEANUP", e, mapOf("connectionId" to connectionId))
                message.fail(500, e.message ?: "Error during socket cleanup")
            }
        }
    }

    /**
     * Handle a new command socket connection
     */
    private suspend fun handleCommandSocket(websocket: ServerWebSocket, traceId: String) {
        log.info("New command WebSocket connection from {}", websocket.remoteAddress())

        // Set up message handler for connection initialization or reconnect
        var handshakeCompleted = false

        websocket.textMessageHandler { message ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    // Only process the first message before handshake completes
                    if (!handshakeCompleted) {
                        val msg = JsonObject(message)
                        if (msg.containsKey("reconnect") && msg.containsKey("connectionId")) {
                            // This is a reconnection attempt
                            val connectionId = msg.getString("connectionId")
                            handshakeCompleted = true
                            handleReconnection(websocket, connectionId, traceId)
                        } else {
                            // For regular messages after handshake, use the standard handler
                            val msg = JsonObject(message)
                            handleMessage(websocket, msg)
                        }
                    } else {
                        // Regular message handling after handshake is complete
                        val msg = JsonObject(message)
                        handleMessage(websocket, msg)
                    }
                } catch (e: Exception) {
                    WebSocketUtils.sendErrorResponse(
                        websocket, e,
                        connectionTracker.getConnectionId(websocket), vlog
                    )
                }
            }
        }

        websocket.closeHandler {
            CoroutineScope(vertx.dispatcher()).launch {
                handleClose(websocket)
            }
        }

        websocket.accept()
        vlog.getEventLogger().trace(
            "WEBSOCKET_ACCEPTED", mapOf(
                "socketId" to websocket.textHandlerID()
            ), traceId
        )

        // If no reconnection message is received, create a new connection
        // Extended handshake timer to account for network latency
        vertx.setTimer(HANDSHAKE_TIMEOUT_MS) {
            if (!handshakeCompleted) {
                vlog.getEventLogger().trace(
                    "HANDSHAKE_TIMEOUT", mapOf(
                        "socketId" to websocket.textHandlerID(),
                        "timeoutMs" to HANDSHAKE_TIMEOUT_MS
                    ), traceId
                )

                handshakeCompleted = true
                CoroutineScope(vertx.dispatcher()).launch {
                    try {
                        initiateConnection(websocket, traceId)
                    } catch (e: Exception) {
                        vlog.logVerticleError(
                            "INITIATE_CONNECTION", e, mapOf(
                                "socketId" to websocket.textHandlerID()
                            )
                        )
                        WebSocketUtils.sendErrorResponse(websocket, e, null, vlog)
                    }
                }
            }
        }
    }

    /**
     * Handle a reconnection attempt
     */
    private suspend fun handleReconnection(websocket: ServerWebSocket, connectionId: String, traceId: String) {
        // Create a trace for this reconnection
        val reconnectTraceId = vlog.getEventLogger().trace(
            "RECONNECTION_ATTEMPT", mapOf(
                "connectionId" to connectionId,
                "socketId" to websocket.textHandlerID(),
                "remoteAddress" to websocket.remoteAddress().toString()
            )
        )

        try {
            vlog.logStartupStep(
                "HANDLING_RECONNECTION", mapOf(
                    "connectionId" to connectionId,
                    "traceId" to reconnectTraceId
                )
            )

            // Check if connection exists and is reconnectable
            val exists = connectionStore.exists(connectionId)
            if (!exists) {
                vlog.getEventLogger().trace(
                    "RECONNECTION_FAILED", mapOf(
                        "reason" to "Connection not found",
                        "connectionId" to connectionId
                    ), reconnectTraceId
                )

                sendReconnectionResponse(websocket, false, "Connection not found")
                initiateConnection(websocket, traceId)  // Fallback to creating a new connection
                return
            }

            // Find the connection to check reconnectable flag
            val connection = connectionStore.find(connectionId)

            if (!connection.reconnectable) {
                vlog.getEventLogger().trace(
                    "RECONNECTION_FAILED", mapOf(
                        "reason" to "Connection not reconnectable",
                        "connectionId" to connectionId
                    ), reconnectTraceId
                )

                sendReconnectionResponse(websocket, false, "Connection not reconnectable")
                initiateConnection(websocket, traceId)  // Fallback to creating a new connection
                return
            }

            // Check last activity timestamp to see if within reconnection window
            val now = System.currentTimeMillis()
            val inactiveMs = now - connection.lastActivity
            val reconnectWindowMs = CleanupVerticle.CONNECTION_RECONNECT_WINDOW_MS

            if (inactiveMs > reconnectWindowMs) {
                vlog.getEventLogger().trace(
                    "RECONNECTION_FAILED", mapOf(
                        "reason" to "Reconnection window expired",
                        "connectionId" to connectionId,
                        "inactiveMs" to inactiveMs,
                        "windowMs" to reconnectWindowMs
                    ), reconnectTraceId
                )

                sendReconnectionResponse(websocket, false, "Reconnection window expired")
                initiateConnection(websocket, traceId)  // Fallback to creating a new connection
                return
            }

            // Close existing command socket if any
            connectionCache.getCommandSocket(connectionId)?.let { existingSocket ->
                try {
                    if (!existingSocket.isClosed()) {
                        existingSocket.close()
                        vlog.getEventLogger().logWebSocketEvent(
                            "EXISTING_SOCKET_CLOSED", connectionId,
                            mapOf("socketId" to existingSocket.textHandlerID()), reconnectTraceId
                        )
                    }
                } catch (e: Exception) {
                    vlog.logVerticleError(
                        "CLOSE_EXISTING_SOCKET", e, mapOf(
                            "connectionId" to connectionId
                        )
                    )
                }
            }

            // Associate the new websocket with this connection
            connectionCache.storeCommandSocket(connectionId, websocket, connection)
            connectionTracker.registerConnection(connectionId, reconnectTraceId, websocket)

            // Update the connection record with new socket ID and activity time
            val socketId = "cs-${java.util.UUID.randomUUID()}"
            val updatedConnection = connectionStore.updateSocketIds(
                connectionId,
                socketId,
                connection.updateSocketId
            )

            vlog.getEventLogger().logStateChange(
                "Connection", "DISCONNECTED", "RECONNECTED",
                mapOf("connectionId" to connectionId, "socketId" to socketId), reconnectTraceId
            )

            // Send confirmation to the client
            sendReconnectionResponse(websocket, true, null)

            // Start heartbeat for this connection
            heartbeatManager.startHeartbeat(connectionId, websocket)

            // If connection has both sockets again, mark as fully connected
            if (connectionCache.isFullyConnected(connectionId)) {
                vlog.getEventLogger().logStateChange(
                    "Connection", "RECONNECTED", "FULLY_CONNECTED",
                    mapOf("connectionId" to connectionId), reconnectTraceId
                )

                connectionController.onClientFullyConnected(updatedConnection)
            }

            vlog.getEventLogger().endTrace(
                reconnectTraceId, "RECONNECTION_COMPLETED",
                mapOf("connectionId" to connectionId, "success" to true)
            )

        } catch (e: Exception) {
            vlog.logVerticleError(
                "RECONNECTION", e, mapOf(
                    "connectionId" to connectionId
                )
            )

            vlog.getEventLogger().logError(
                "RECONNECTION_ERROR", e,
                mapOf("connectionId" to connectionId), reconnectTraceId
            )

            sendReconnectionResponse(websocket, false, "Internal error")
            initiateConnection(websocket, traceId)  // Fallback to creating a new connection
        }
    }

    /**
     * Send a reconnection response to the client
     */
    private fun sendReconnectionResponse(websocket: ServerWebSocket, success: Boolean, errorMessage: String?) {
        val response = JsonObject()
            .put("type", "reconnect_response")
            .put("success", success)
            .put("timestamp", System.currentTimeMillis())

        if (!success && errorMessage != null) {
            response.put("error", errorMessage)
        }

        websocket.writeTextMessage(response.encode())
    }

    /**
     * Connect to the command socket
     */
    private suspend fun initiateConnection(websocket: ServerWebSocket, traceId: String) {
        try {
            // Log DB operation about to happen
            vlog.getEventLogger().logDbOperation("CREATE", "connections", emptyMap(), traceId)

            val startTime = System.currentTimeMillis()
            val connection = connectionStore.create()

            // Log performance metrics
            val createTime = System.currentTimeMillis() - startTime
            vlog.getEventLogger().logPerformance(
                "CREATE_CONNECTION", createTime,
                mapOf("connectionId" to connection._id), traceId
            )

            // Store in cache
            connectionCache.storeConnection(connection)
            connectionTracker.registerConnection(connection._id, traceId, websocket)

            // Log connection creation event
            vlog.getEventLogger().logStateChange(
                "Connection", "NONE", "CREATED",
                mapOf("connectionId" to connection._id), traceId
            )

            // Generate a command socket ID
            val commandSocketId = "cs-${java.util.UUID.randomUUID()}"

            // Log DB operation about to happen
            vlog.getEventLogger().logDbOperation(
                "UPDATE", "connections",
                mapOf("connectionId" to connection._id, "action" to "update_socket_ids"), traceId
            )

            // Update the connection in the database
            val updatedConnection = connectionStore.updateSocketIds(
                connection._id,
                commandSocketId,
                null
            )

            // IMPORTANT: Update the cache with the updated connection
            connectionCache.storeConnection(updatedConnection)

            // Register command socket
            connectionCache.storeCommandSocket(connection._id, websocket, updatedConnection)

            // Log successful socket registration
            vlog.getEventLogger().logWebSocketEvent(
                "SOCKET_REGISTERED", connection._id,
                mapOf("socketType" to "command", "socketId" to commandSocketId), traceId
            )

            WebSocketUtils.sendConfirmation(websocket, "connection_confirm", connection._id)

            // Start heartbeat for this connection
            heartbeatManager.startHeartbeat(connection._id, websocket)

            // Log successful connection completion
            vlog.getEventLogger().trace(
                "CONNECTION_ESTABLISHED",
                mapOf("connectionId" to connection._id), traceId
            )
        } catch (e: Exception) {
            vlog.getEventLogger().logError("CONNECTION_FAILED", e, emptyMap(), traceId)
            WebSocketUtils.sendErrorResponse(websocket, e, null, vlog)
        }
    }

    /**
     * Handle an incoming message from a client
     */
    private suspend fun handleMessage(websocket: ServerWebSocket, message: JsonObject) {
        val connectionId = connectionTracker.getConnectionId(websocket)
        if (connectionId == null) {
            WebSocketUtils.sendErrorResponse(
                websocket,
                IllegalStateException("No connection found for this websocket"), null, vlog
            )
            return
        }

        val traceId = connectionTracker.getTraceId(connectionId)
        val msgTraceId = vlog.getEventLogger().trace(
            "MESSAGE_RECEIVED", mapOf(
                "messageType" to message.getString("type", "unknown"),
                "connectionId" to connectionId
            ), traceId
        )

        try {
            // Update last activity timestamp
            connectionStore.updateLastActivity(connectionId)

            // Check for heartbeat response
            if (message.getString("type") == "heartbeat_response") {
                heartbeatManager.handleHeartbeatResponse(connectionId, message)
            } else {
                // Process regular message
                messageHandler.handle(connectionId, message)
            }

            vlog.getEventLogger().trace(
                "MESSAGE_PROCESSED", mapOf(
                    "messageType" to message.getString("type", "unknown"),
                    "connectionId" to connectionId
                ), msgTraceId
            )
        } catch (e: Exception) {
            vlog.logVerticleError("MESSAGE_HANDLING", e)
            WebSocketUtils.sendErrorResponse(websocket, e, connectionId, vlog)
        }
    }

    /**
     * Handle a socket being closed
     */
    private suspend fun handleClose(websocket: ServerWebSocket) {
        val connectionId = connectionTracker.getConnectionId(websocket)
        if (connectionId == null) return

        val traceId = connectionTracker.getTraceId(connectionId)

        try {
            // Stop the heartbeat
            heartbeatManager.stopHeartbeat(connectionId)

            // Set it as reconnectable for the reconnection window
            connectionStore.updateReconnectable(connectionId, true)

            // Remove socket from cache but don't delete connection yet
            connectionCache.removeCommandSocket(connectionId)
            connectionTracker.handleSocketClosed(websocket)

            vlog.getEventLogger().logStateChange(
                "Connection", "CONNECTED", "DISCONNECTED",
                mapOf("connectionId" to connectionId), traceId
            )

            vlog.getEventLogger().trace(
                "RECONNECTION_WINDOW_STARTED", mapOf(
                    "connectionId" to connectionId,
                    "windowMs" to CleanupVerticle.CONNECTION_RECONNECT_WINDOW_MS
                ), traceId
            )
        } catch (e: Exception) {
            vlog.logVerticleError("WEBSOCKET_CLOSE", e, mapOf("connectionId" to connectionId))
        }
    }

    /**
     * Handle entity-specific sync request
     */
    private suspend fun handleEntitySync(connectionId: String, entityId: String): Boolean {
        val traceId = connectionTracker.getTraceId(connectionId)

        try {
            vlog.getEventLogger().trace(
                "ENTITY_SYNC_STARTED", mapOf(
                    "entityId" to entityId,
                    "connectionId" to connectionId
                ), traceId
            )

            // Check if connection and command socket exist
            if (!connectionCache.hasConnection(connectionId)) {
                vlog.getEventLogger().trace(
                    "CONNECTION_NOT_FOUND", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
                return false
            }

            val commandSocket = connectionCache.getCommandSocket(connectionId)
            if (commandSocket == null || commandSocket.isClosed()) {
                vlog.getEventLogger().trace(
                    "COMMAND_SOCKET_UNAVAILABLE", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
                return false
            }

            // Fetch the entity
            val startTime = System.currentTimeMillis()
            vlog.getEventLogger().logDbOperation(
                "FIND", "entities",
                mapOf("entityId" to entityId), traceId
            )

            val entities = entityStore.findEntitiesByIds(listOf(entityId))

            val queryTime = System.currentTimeMillis() - startTime
            vlog.getEventLogger().logPerformance(
                "ENTITY_QUERY", queryTime,
                mapOf("entityId" to entityId), traceId
            )

            if (entities.isEmpty()) {
                vlog.getEventLogger().trace(
                    "ENTITY_NOT_FOUND", mapOf(
                        "entityId" to entityId
                    ), traceId
                )
                sendSyncErrorToClient(connectionId, "Entity not found")
                return false
            }

            val entity = entities[entityId]

            // Create a sync response message
            val syncData = JsonObject()
                .put(
                    "entities", JsonObject()
                        .put("_id", entity?._id)
                        .put("type", entity?.type)
                        .put("version", entity?.version)
                    // Add more entity fields as needed
                )
                .put("timestamp", System.currentTimeMillis())

            val syncMessage = JsonObject()
                .put("type", "entity_sync")
                .put("data", syncData)

            // Send the sync message to the client
            commandSocket.writeTextMessage(syncMessage.encode())

            vlog.getEventLogger().trace(
                "ENTITY_SYNC_DATA_SENT", mapOf(
                    "entityId" to entityId,
                    "connectionId" to connectionId
                ), traceId
            )

            // Update last activity
            connectionStore.updateLastActivity(connectionId)

            vlog.getEventLogger().trace(
                "ENTITY_SYNC_COMPLETED", mapOf(
                    "entityId" to entityId,
                    "connectionId" to connectionId
                ), traceId
            )

            return true
        } catch (e: Exception) {
            vlog.logVerticleError(
                "ENTITY_SYNC", e, mapOf(
                    "entityId" to entityId,
                    "connectionId" to connectionId
                )
            )
            sendSyncErrorToClient(connectionId, "Sync failed: ${e.message}")
            return false
        }
    }

    /**
     * Send a sync error message to the client
     */
    private fun sendSyncErrorToClient(connectionId: String, errorMessage: String) {
        val socket = connectionCache.getCommandSocket(connectionId)
        if (socket != null && !socket.isClosed()) {
            try {
                val errorResponse = JsonObject()
                    .put("type", "sync_error")
                    .put("error", errorMessage)
                    .put("timestamp", System.currentTimeMillis())

                socket.writeTextMessage(errorResponse.encode())

                vlog.getEventLogger().trace(
                    "SYNC_ERROR_SENT", mapOf(
                        "connectionId" to connectionId,
                        "error" to errorMessage
                    )
                )
            } catch (e: Exception) {
                vlog.logVerticleError(
                    "SYNC_ERROR_SEND", e, mapOf(
                        "connectionId" to connectionId
                    )
                )
            }
        }
    }

    /**
     * Coordinated connection cleanup process
     */
    private suspend fun cleanupConnection(connectionId: String): Boolean {
        val traceId = connectionTracker.getTraceId(connectionId)

        try {
            vlog.getEventLogger().trace(
                "CONNECTION_CLEANUP_STARTED", mapOf(
                    "connectionId" to connectionId
                ), traceId
            )

            // Step 1: Clean up channels
            val channelCleanupResult = cleanupConnectionChannels(connectionId)
            if (!channelCleanupResult) {
                vlog.getEventLogger().trace(
                    "CHANNEL_CLEANUP_FAILED", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
            }

            // Step 2: Clean up sockets and stop heartbeat
            heartbeatManager.stopHeartbeat(connectionId)
            val updateSocket = connectionCache.getUpdateSocket(connectionId)
            val socketCleanupResult = cleanupConnectionSockets(connectionId, updateSocket != null)
            if (!socketCleanupResult) {
                vlog.getEventLogger().trace(
                    "SOCKET_CLEANUP_FAILED", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
            }

            // Step 3: Mark connection as not reconnectable
            try {
                connectionStore.updateReconnectable(connectionId, false)
                vlog.getEventLogger().logStateChange(
                    "Connection", "DISCONNECTED", "NOT_RECONNECTABLE",
                    mapOf("connectionId" to connectionId), traceId
                )
            } catch (e: Exception) {
                vlog.logVerticleError(
                    "SET_NOT_RECONNECTABLE", e, mapOf(
                        "connectionId" to connectionId
                    )
                )
            }

            // Step 4: Remove the connection from DB and cache
            try {
                // Try to delete from the database first
                connectionStore.delete(connectionId)
                vlog.getEventLogger().logDbOperation(
                    "DELETE", "connections",
                    mapOf("connectionId" to connectionId), traceId
                )
            } catch (e: Exception) {
                vlog.logVerticleError(
                    "DB_CONNECTION_DELETE", e, mapOf(
                        "connectionId" to connectionId
                    )
                )
                // Continue anyway - the connection might already be gone
            }

            // Final cleanup from cache
            try {
                connectionCache.removeConnection(connectionId)
                connectionTracker.removeConnection(connectionId)
                vlog.getEventLogger().trace(
                    "CACHE_CONNECTION_REMOVED", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
            } catch (e: Exception) {
                vlog.logVerticleError(
                    "CACHE_CONNECTION_REMOVE", e, mapOf(
                        "connectionId" to connectionId
                    )
                )
            }

            vlog.getEventLogger().trace(
                "CONNECTION_CLEANUP_COMPLETED", mapOf(
                    "connectionId" to connectionId
                ), traceId
            )

            return true
        } catch (e: Exception) {
            vlog.logVerticleError(
                "CONNECTION_CLEANUP", e, mapOf(
                    "connectionId" to connectionId
                )
            )

            // Do emergency cleanup as a last resort
            try {
                connectionCache.removeCommandSocket(connectionId)
                connectionCache.removeUpdateSocket(connectionId)
                connectionCache.removeConnection(connectionId)
                connectionTracker.removeConnection(connectionId)
                vlog.getEventLogger().trace(
                    "EMERGENCY_CLEANUP_COMPLETED", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
            } catch (innerEx: Exception) {
                vlog.logVerticleError(
                    "EMERGENCY_CLEANUP", innerEx, mapOf(
                        "connectionId" to connectionId
                    )
                )
            }

            return false
        }
    }

    /**
     * Cleans up channel memberships for a connection
     */
    private suspend fun cleanupConnectionChannels(connectionId: String): Boolean {
        val traceId = connectionTracker.getTraceId(connectionId)

        try {
            // Get the channels this connection is in
            val channels = channelStore.getChannelsForClient(connectionId)

            vlog.getEventLogger().trace("CHANNEL_CLEANUP_STARTED", mapOf(
                "connectionId" to connectionId,
                "channelCount" to channels.size
            ), traceId)

            if (channels.isEmpty()) {
                vlog.getEventLogger().trace("NO_CHANNELS_FOUND", mapOf(
                    "connectionId" to connectionId
                ), traceId)
                return true
            }

            // Remove the connection from each channel
            var success = true
            for (channelId in channels) {
                try {
                    val result = channelStore.removeClientFromChannel(channelId, connectionId)
                    if (!result) {
                        vlog.getEventLogger().trace("CHANNEL_REMOVAL_FAILED", mapOf(
                            "connectionId" to connectionId,
                            "channelId" to channelId
                        ), traceId)
                        success = false
                    } else {
                        vlog.getEventLogger().trace("CHANNEL_REMOVAL_SUCCEEDED", mapOf(
                            "connectionId" to connectionId,
                            "channelId" to channelId
                        ), traceId)
                    }
                } catch (e: Exception) {
                    vlog.logVerticleError("CHANNEL_REMOVAL", e, mapOf(
                        "connectionId" to connectionId,
                        "channelId" to channelId
                    ))
                    success = false
                }
            }

            vlog.getEventLogger().trace("CHANNEL_CLEANUP_COMPLETED", mapOf(
                "connectionId" to connectionId,
                "success" to success
            ), traceId)

            return success
        } catch (e: Exception) {
            vlog.logVerticleError("CHANNEL_CLEANUP", e, mapOf(
                "connectionId" to connectionId
            ))
            return false
        }
    }

    /**
     * Cleans up sockets for a connection
     */
    private suspend fun cleanupConnectionSockets(connectionId: String, hasUpdateSocket: Boolean): Boolean {
        val traceId = connectionTracker.getTraceId(connectionId)

        try {
            var success = true

            vlog.getEventLogger().trace("SOCKET_CLEANUP_STARTED", mapOf(
                "connectionId" to connectionId,
                "hasUpdateSocket" to hasUpdateSocket
            ), traceId)

            // Clean up command socket
            try {
                connectionCache.removeCommandSocket(connectionId)?.let { socket ->
                    if (!socket.isClosed()) {
                        try {
                            socket.close()
                            vlog.getEventLogger().trace("COMMAND_SOCKET_CLOSED", mapOf(
                                "connectionId" to connectionId,
                                "socketId" to socket.textHandlerID()
                            ), traceId)
                        } catch (e: Exception) {
                            vlog.logVerticleError("COMMAND_SOCKET_CLOSE", e, mapOf(
                                "connectionId" to connectionId
                            ))
                        }
                    }
                }
            } catch (e: Exception) {
                vlog.logVerticleError("COMMAND_SOCKET_CLEANUP", e, mapOf(
                    "connectionId" to connectionId
                ))
                success = false
            }

            // Clean up update socket if it exists
            if (hasUpdateSocket) {
                try {
                    connectionCache.removeUpdateSocket(connectionId)?.let { socket ->
                        if (!socket.isClosed()) {
                            try {
                                socket.close()
                                vlog.getEventLogger().trace("UPDATE_SOCKET_CLOSED", mapOf(
                                    "connectionId" to connectionId,
                                    "socketId" to socket.textHandlerID()
                                ), traceId)
                            } catch (e: Exception) {
                                vlog.logVerticleError("UPDATE_SOCKET_CLOSE", e, mapOf(
                                    "connectionId" to connectionId
                                ))
                            }
                        }
                    }
                } catch (e: Exception) {
                    vlog.logVerticleError("UPDATE_SOCKET_CLEANUP", e, mapOf(
                        "connectionId" to connectionId
                    ))
                    success = false
                }
            }

            vlog.getEventLogger().trace("SOCKET_CLEANUP_COMPLETED", mapOf(
                "connectionId" to connectionId,
                "success" to success
            ), traceId)

            return success
        } catch (e: Exception) {
            vlog.logVerticleError("SOCKET_CLEANUP", e, mapOf(
                "connectionId" to connectionId
            ))
            return false
        }
    }

    override suspend fun stop() {
        vlog.logStartupStep("STOPPING")

        // Stop all heartbeats
        heartbeatManager.stopAll()

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
    }
}