package com.minare.worker

import com.minare.cache.ConnectionCache
import com.minare.controller.ConnectionController
import com.minare.core.entity.ReflectionCache
import com.minare.core.websocket.CommandMessageHandler
import com.minare.persistence.ConnectionStore
import com.minare.persistence.ChannelStore
import com.minare.persistence.ContextStore
import com.minare.persistence.EntityStore
import com.minare.utils.EventLogger
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.Handler
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.RoutingContext
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
class CommandSocketVerticle @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache,
    private val connectionController: ConnectionController,
    private val channelStore: ChannelStore,
    private val contextStore: ContextStore,
    private val messageHandler: CommandMessageHandler,
    private val reflectionCache: ReflectionCache,
    private val entityStore: EntityStore
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(CommandSocketVerticle::class.java)
    private lateinit var vlog: VerticleLogger
    private lateinit var eventBusUtils: EventBusUtils

    // Map to store heartbeat timer IDs
    private val heartbeatTimers = mutableMapOf<String, Long>()

    // Router for command socket endpoints
    private lateinit var router: Router

    // HTTP server if we're using our own
    private var httpServer: HttpServer? = null

    // Map to store trace IDs for active connections
    private val connectionTraces = mutableMapOf<String, String>()

    // Track deployment data
    private var deployedAt: Long = 0
    private var httpServerVerticleId: String? = null
    private var useOwnHttpServer: Boolean = false
    private var httpServerPort: Int = 8081 // Default port if using own server

    companion object {
        // Event bus addresses
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

        // Heartbeat interval of 15 seconds
        const val HEARTBEAT_INTERVAL_MS = 15000L

        // Base path for command socket routes
        const val BASE_PATH = "/ws"
    }

    override suspend fun start() {
        log.info("Starting CommandSocketVerticle")

        // Record deployment time
        deployedAt = System.currentTimeMillis()

        // Initialize logging utilities
        vlog = VerticleLogger(this)
        eventBusUtils = vlog.createEventBusUtils()

        vlog.logStartupStep("STARTING")

        // Create router regardless of whether we use own HTTP server
        router = Router.router(vertx)
        vlog.logStartupStep("ROUTER_CREATED")

        // Check if we should use our own HTTP server
        useOwnHttpServer = config.getBoolean("useOwnHttpServer", false)

        // Log configuration in a safe way
        vlog.logConfig(config)

        log.info("CommandSocketVerticle configured with useOwnHttpServer={}", useOwnHttpServer)

        // Register event bus handler for initialization
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_COMMAND_SOCKET_INITIALIZE) { message, traceId ->
            try {
                vlog.logStartupStep("INITIALIZING_ROUTER", mapOf("traceId" to traceId))

                val startTime = System.currentTimeMillis()
                initializeRouter()
                val initTime = System.currentTimeMillis() - startTime

                vlog.logVerticlePerformance("ROUTER_INITIALIZATION", initTime)

                // If using own HTTP server, deploy it now
                if (useOwnHttpServer) {
                    deployOwnHttpServer()
                }

                val reply = JsonObject()
                    .put("success", true)
                    .put("message", "Command socket router initialized" +
                            (if (useOwnHttpServer) " with dedicated HTTP server on port $httpServerPort" else ""))

                eventBusUtils.tracedReply(message, reply, traceId)

                vlog.logStartupStep("ROUTER_INITIALIZED", mapOf(
                    "status" to "success",
                    "initTime" to initTime,
                    "useOwnHttpServer" to useOwnHttpServer
                ))
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
                vlog.logStartupStep("ENTITY_SYNC_REQUEST", mapOf(
                    "entityId" to entityId,
                    "connectionId" to connectionId,
                    "traceId" to traceId
                ))

                val result = handleEntitySync(connectionId, entityId)

                eventBusUtils.tracedReply(message, JsonObject().put("success", result), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError("ENTITY_SYNC", e, mapOf(
                    "entityId" to entityId,
                    "connectionId" to connectionId
                ))
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
            vlog.logStartupStep("SOCKET_CLEANUP_REQUEST", mapOf(
                "connectionId" to connectionId,
                "hasUpdateSocket" to hasUpdateSocket
            ))

            try {
                val result = cleanupConnectionSockets(connectionId, hasUpdateSocket)
                eventBusUtils.tracedReply(message, JsonObject().put("success", result), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError("SOCKET_CLEANUP", e, mapOf("connectionId" to connectionId))
                message.fail(500, e.message ?: "Error during socket cleanup")
            }
        }

        // Save deployment ID
        deploymentID?.let {
            vlog.logDeployment(it)
        }

        vlog.logStartupStep("STARTED")
    }

    /**
     * Deploy a dedicated HTTP server for command sockets
     */
    private suspend fun deployOwnHttpServer() {
        vlog.logStartupStep("DEPLOYING_OWN_HTTP_SERVER")

        try {
            // Get configuration - use a different port than main server
            httpServerPort = config.getInteger("httpPort", 8080)
            val httpHost = config.getString("httpHost", "0.0.0.0")

            log.info("Starting own HTTP server on $httpHost:$httpServerPort")

            // Create HTTP server options
            val options = HttpServerOptions()
                .setHost(httpHost)
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
                "host" to httpHost
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
     * Initialize the router with command socket routes
     */
    private fun initializeRouter() {
        vlog.logStartupStep("INITIALIZING_ROUTER")

        // Add a debug endpoint to check if router is working
        router.get("/ws-debug").handler { ctx ->
            ctx.response()
                .putHeader("Content-Type", "application/json")
                .end(JsonObject()
                    .put("status", "ok")
                    .put("message", "CommandSocketVerticle router is working")
                    .put("timestamp", System.currentTimeMillis())
                    .encode())
        }

        // Setup main WebSocket route for commands
        log.info("Setting up websocket route handler at path: {}", BASE_PATH)

        router.route("$BASE_PATH").handler { context ->
            log.info("Received request to WebSocket path: {}", context.request().path())

            val traceId = vlog.getEventLogger().trace("WEBSOCKET_ROUTE_ACCESSED", mapOf(
                "path" to BASE_PATH,
                "remoteAddress" to context.request().remoteAddress().toString()
            ))

            context.request().toWebSocket()
                .onSuccess { socket ->
                    // Handle the WebSocket directly in this verticle
                    log.info("WebSocket upgrade successful for client: {}", socket.remoteAddress())

                    launch {
                        try {
                            vlog.getEventLogger().trace("WEBSOCKET_UPGRADED", mapOf(
                                "socketId" to socket.textHandlerID()
                            ), traceId)

                            handleCommandSocket(socket)
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
                        .end("Command WebSocket upgrade failed: ${err.message}")
                }
        }

        // Add health check route
        router.get("$BASE_PATH/health").handler { ctx ->
            // Get connection stats
            val connectionCount = connectionCache.getConnectionCount()
            val fullyConnectedCount = connectionCache.getFullyConnectedCount()

            // Create response with detailed metrics
            val healthInfo = JsonObject()
                .put("status", "ok")
                .put("verticle", this.javaClass.simpleName)
                .put("deploymentId", deploymentID)
                .put("connections", connectionCount)
                .put("fullyConnected", fullyConnectedCount)
                .put("timestamp", System.currentTimeMillis())
                .put("uptime", System.currentTimeMillis() - deployedAt)
                .put("metrics", JsonObject()
                    .put("heartbeatTimers", heartbeatTimers.size)
                    .put("connectionTraces", connectionTraces.size)
                )

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(healthInfo.encode())

            // Log health check access with low frequency
            if (Math.random() < 0.1) { // Only log ~10% of health checks to avoid log spam
                vlog.logStartupStep("HEALTH_CHECK_ACCESSED", mapOf(
                    "connections" to connectionCount,
                    "fullyConnected" to fullyConnectedCount
                ))
            }
        }

        // Add metrics endpoint
        router.get("$BASE_PATH/metrics").handler { ctx ->
            val metrics = JsonObject()
                .put("connections", JsonObject()
                    .put("total", connectionCache.getConnectionCount())
                    .put("fullyConnected", connectionCache.getFullyConnectedCount())
                )
                .put("heartbeats", JsonObject()
                    .put("active", heartbeatTimers.size)
                )

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(metrics.encode())
        }

        vlog.logStartupStep("ROUTER_INITIALIZED")
        log.info("Command socket router initialized with routes: /ws, /ws/health, /ws/metrics, /ws-debug")
    }

    /**
     * Get the router for command socket routes
     */
    fun getRouter(): Router {
        return router
    }

    /**
     * Handle a new command socket connection
     * Enhanced to support reconnection of existing connections
     */
    suspend fun handleCommandSocket(websocket: ServerWebSocket) {
        val remoteAddress = websocket.remoteAddress().toString()
        log.info("New command socket connection from {}", remoteAddress)

        // Create a trace for this connection
        val connectionTraceId = vlog.getEventLogger().logWebSocketEvent(
            "SOCKET_CONNECTED",
            null,
            mapOf(
                "remoteAddress" to remoteAddress,
                "socketId" to websocket.textHandlerID()
            )
        )

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
                            handleReconnection(websocket, connectionId)
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
                    handleError(websocket, e)
                }
            }
        }

        websocket.closeHandler {
            CoroutineScope(vertx.dispatcher()).launch {
                handleClose(websocket)
            }
        }

        websocket.accept()
        vlog.getEventLogger().trace("WEBSOCKET_ACCEPTED", mapOf(
            "socketId" to websocket.textHandlerID()
        ), connectionTraceId)

        // If no reconnection message is received, create a new connection
        // Extended handshake timer to account for network latency
        vertx.setTimer(HANDSHAKE_TIMEOUT_MS) {
            if (!handshakeCompleted) {
                vlog.getEventLogger().trace("HANDSHAKE_TIMEOUT", mapOf(
                    "socketId" to websocket.textHandlerID(),
                    "timeoutMs" to HANDSHAKE_TIMEOUT_MS
                ), connectionTraceId)

                handshakeCompleted = true
                CoroutineScope(vertx.dispatcher()).launch {
                    try {
                        initiateConnection(websocket)
                    } catch (e: Exception) {
                        vlog.logVerticleError("INITIATE_CONNECTION", e, mapOf(
                            "socketId" to websocket.textHandlerID()
                        ))
                        handleError(websocket, e)
                    }
                }
            }
        }
    }

    /**
     * Handle a reconnection attempt
     */
    private suspend fun handleReconnection(websocket: ServerWebSocket, connectionId: String) {
        // Create a trace for this reconnection
        val reconnectTraceId = vlog.getEventLogger().trace("RECONNECTION_ATTEMPT", mapOf(
            "connectionId" to connectionId,
            "socketId" to websocket.textHandlerID(),
            "remoteAddress" to websocket.remoteAddress().toString()
        ))

        try {
            vlog.logStartupStep("HANDLING_RECONNECTION", mapOf(
                "connectionId" to connectionId,
                "traceId" to reconnectTraceId
            ))

            // Check if connection exists and is reconnectable
            val exists = connectionStore.exists(connectionId)
            if (!exists) {
                vlog.getEventLogger().trace("RECONNECTION_FAILED", mapOf(
                    "reason" to "Connection not found",
                    "connectionId" to connectionId
                ), reconnectTraceId)

                sendReconnectionResponse(websocket, false, "Connection not found")
                initiateConnection(websocket)  // Fallback to creating a new connection
                return
            }

            // Find the connection to check reconnectable flag
            val connection = connectionStore.find(connectionId)

            if (!connection.reconnectable) {
                vlog.getEventLogger().trace("RECONNECTION_FAILED", mapOf(
                    "reason" to "Connection not reconnectable",
                    "connectionId" to connectionId
                ), reconnectTraceId)

                sendReconnectionResponse(websocket, false, "Connection not reconnectable")
                initiateConnection(websocket)  // Fallback to creating a new connection
                return
            }

            // Check last activity timestamp to see if within reconnection window
            val now = System.currentTimeMillis()
            val inactiveMs = now - connection.lastActivity
            val reconnectWindowMs = CleanupVerticle.CONNECTION_RECONNECT_WINDOW_MS

            if (inactiveMs > reconnectWindowMs) {
                vlog.getEventLogger().trace("RECONNECTION_FAILED", mapOf(
                    "reason" to "Reconnection window expired",
                    "connectionId" to connectionId,
                    "inactiveMs" to inactiveMs,
                    "windowMs" to reconnectWindowMs
                ), reconnectTraceId)

                sendReconnectionResponse(websocket, false, "Reconnection window expired")
                initiateConnection(websocket)  // Fallback to creating a new connection
                return
            }

            // Close existing command socket if any
            connectionCache.getCommandSocket(connectionId)?.let { existingSocket ->
                try {
                    if (!existingSocket.isClosed()) {
                        existingSocket.close()
                        vlog.getEventLogger().logWebSocketEvent("EXISTING_SOCKET_CLOSED", connectionId,
                            mapOf("socketId" to existingSocket.textHandlerID()), reconnectTraceId)
                    }
                } catch (e: Exception) {
                    vlog.logVerticleError("CLOSE_EXISTING_SOCKET", e, mapOf(
                        "connectionId" to connectionId
                    ))
                }
            }

            // Associate the new websocket with this connection
            connectionCache.storeCommandSocket(connectionId, websocket, connection)

            // Update the connection record with new socket ID and activity time
            val socketId = "cs-${java.util.UUID.randomUUID()}"
            val updatedConnection = connectionStore.updateSocketIds(
                connectionId,
                socketId,
                connection.updateSocketId
            )

            vlog.getEventLogger().logStateChange("Connection", "DISCONNECTED", "RECONNECTED",
                mapOf("connectionId" to connectionId, "socketId" to socketId), reconnectTraceId)

            // Send confirmation to the client
            sendReconnectionResponse(websocket, true, null)

            // Start heartbeat for this connection
            startHeartbeat(connectionId)

            // Update the stored trace ID
            connectionTraces[connectionId] = reconnectTraceId

            // If connection has both sockets again, mark as fully connected
            if (connectionCache.isFullyConnected(connectionId)) {
                vlog.getEventLogger().logStateChange("Connection", "RECONNECTED", "FULLY_CONNECTED",
                    mapOf("connectionId" to connectionId), reconnectTraceId)

                connectionController.onClientFullyConnected(updatedConnection)
            }

            vlog.getEventLogger().endTrace(reconnectTraceId, "RECONNECTION_COMPLETED",
                mapOf("connectionId" to connectionId, "success" to true))

        } catch (e: Exception) {
            vlog.logVerticleError("RECONNECTION", e, mapOf(
                "connectionId" to connectionId
            ))

            vlog.getEventLogger().logError("RECONNECTION_ERROR", e,
                mapOf("connectionId" to connectionId), reconnectTraceId)

            sendReconnectionResponse(websocket, false, "Internal error")
            initiateConnection(websocket)  // Fallback to creating a new connection
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
    private suspend fun initiateConnection(websocket: ServerWebSocket) {
        // Get any existing trace ID for this websocket
        val connectionTraceId = vlog.getEventLogger().trace("INITIATE_CONNECTION", mapOf(
            "remoteAddress" to websocket.remoteAddress().toString(),
            "socketId" to websocket.textHandlerID()
        ))

        try {
            // Log DB operation about to happen
            vlog.getEventLogger().logDbOperation("CREATE", "connections", emptyMap(), connectionTraceId)

            val startTime = System.currentTimeMillis()
            val connection = connectionStore.create()

            // Log performance metrics
            val createTime = System.currentTimeMillis() - startTime
            vlog.getEventLogger().logPerformance("CREATE_CONNECTION", createTime,
                mapOf("connectionId" to connection._id), connectionTraceId)

            // Store in cache - separate from the DB operation
            connectionCache.storeConnection(connection)

            // Store connection trace ID for future use
            connectionTraces[connection._id] = connectionTraceId

            // Log connection creation event
            vlog.getEventLogger().logStateChange("Connection", "NONE", "CREATED",
                mapOf("connectionId" to connection._id), connectionTraceId)

            // Generate a command socket ID
            val commandSocketId = "cs-${java.util.UUID.randomUUID()}"

            // Log DB operation about to happen
            vlog.getEventLogger().logDbOperation("UPDATE", "connections",
                mapOf("connectionId" to connection._id, "action" to "update_socket_ids"), connectionTraceId)

            // Update the connection in the database
            val updatedConnection = connectionStore.updateSocketIds(
                connection._id,
                commandSocketId,
                null
            )

            // IMPORTANT: Update the cache with the updated connection to prevent cache/DB inconsistency
            connectionCache.storeConnection(updatedConnection)

            // Register command socket - now with the updated connection that has the command socket ID
            connectionCache.storeCommandSocket(connection._id, websocket, updatedConnection)

            // Log successful socket registration
            vlog.getEventLogger().logWebSocketEvent("SOCKET_REGISTERED", connection._id,
                mapOf("socketType" to "command", "socketId" to commandSocketId), connectionTraceId)

            sendConnectionConfirmation(websocket, connection._id)

            // Start heartbeat for this connection
            startHeartbeat(connection._id)

            // Log successful connection completion
            vlog.getEventLogger().trace("CONNECTION_ESTABLISHED",
                mapOf("connectionId" to connection._id), connectionTraceId)
        } catch (e: Exception) {
            vlog.getEventLogger().logError("CONNECTION_FAILED", e, emptyMap(), connectionTraceId)
            handleError(websocket, e)
        }
    }

    private fun sendConnectionConfirmation(websocket: ServerWebSocket, connectionId: String) {
        val confirmation = JsonObject()
            .put("type", "connection_confirm")
            .put("connectionId", connectionId)
            .put("timestamp", System.currentTimeMillis())

        websocket.writeTextMessage(confirmation.encode())
        vlog.getEventLogger().logWebSocketEvent("CONNECTION_CONFIRMED", connectionId,
            mapOf("timestamp" to System.currentTimeMillis()))
    }

    private suspend fun handleMessage(websocket: ServerWebSocket, message: JsonObject) {
        try {
            val connectionId = connectionCache.getConnectionIdForCommandSocket(websocket)
            if (connectionId != null) {
                // Get the trace ID for this connection if available
                val traceId = connectionTraces[connectionId]
                val msgTraceId = vlog.getEventLogger().trace("MESSAGE_RECEIVED", mapOf(
                    "messageType" to message.getString("type", "unknown"),
                    "connectionId" to connectionId
                ), traceId)

                // Update last activity timestamp
                connectionStore.updateLastActivity(connectionId)

                // Check for heartbeat response
                if (message.getString("type") == "heartbeat_response") {
                    handleHeartbeatResponse(connectionId, message)
                } else {
                    // Process regular message
                    messageHandler.handle(connectionId, message)
                }

                vlog.getEventLogger().trace("MESSAGE_PROCESSED", mapOf(
                    "messageType" to message.getString("type", "unknown"),
                    "connectionId" to connectionId
                ), msgTraceId)
            } else {
                throw IllegalStateException("No connection found for this websocket")
            }
        } catch (e: Exception) {
            vlog.logVerticleError("MESSAGE_HANDLING", e)
            handleError(websocket, e)
        }
    }

    private suspend fun handleClose(websocket: ServerWebSocket) {
        val connectionId = connectionCache.getConnectionIdForCommandSocket(websocket)

        try {
            if (connectionId != null) {
                val traceId = connectionTraces[connectionId]
                vlog.getEventLogger().logWebSocketEvent("SOCKET_CLOSED", connectionId,
                    mapOf("socketType" to "command"), traceId)

                // Stop the heartbeat
                stopHeartbeat(connectionId)

                // Instead of immediate cleanup, keep connection for potential reconnection
                // Set it as reconnectable for the reconnection window
                connectionStore.updateReconnectable(connectionId, true)

                // Remove socket from cache but don't delete connection yet
                connectionCache.removeCommandSocket(connectionId)

                vlog.getEventLogger().logStateChange("Connection", "CONNECTED", "DISCONNECTED",
                    mapOf("connectionId" to connectionId), traceId)

                // If reconnection doesn't happen, cleanup verticle will handle it later
                vlog.getEventLogger().trace("RECONNECTION_WINDOW_STARTED", mapOf(
                    "connectionId" to connectionId,
                    "windowMs" to CleanupVerticle.CONNECTION_RECONNECT_WINDOW_MS
                ), traceId)
            } else {
                vlog.getEventLogger().trace("ORPHAN_SOCKET_CLOSED", mapOf(
                    "remoteAddress" to websocket.remoteAddress().toString()
                ))
            }
        } catch (e: Exception) {
            if (connectionId != null) {
                vlog.logVerticleError("WEBSOCKET_CLOSE", e, mapOf("connectionId" to connectionId))
            } else {
                vlog.logVerticleError("WEBSOCKET_CLOSE", e)
            }
        }
    }

    private fun handleError(websocket: ServerWebSocket, error: Throwable) {
        val connectionId = connectionCache.getConnectionIdForCommandSocket(websocket)
        val traceId = connectionId?.let { connectionTraces[it] }

        vlog.logVerticleError("WEBSOCKET_ERROR", error, mapOf(
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
            // Don't close the socket here - let the client handle reconnection
            // Only close if it's a critical error during handshake
            if (connectionId == null && !websocket.isClosed()) {
                websocket.close()
            }
        }
    }

    /**
     * Handle a heartbeat response from client
     */
    private fun handleHeartbeatResponse(connectionId: String, message: JsonObject) {
        try {
            // Calculate round-trip time
            val serverTimestamp = message.getLong("timestamp")
            val clientTimestamp = message.getLong("clientTimestamp", 0L)
            val now = System.currentTimeMillis()
            val roundTripTime = now - serverTimestamp

            // Only log occasionally to reduce noise
            if (Math.random() < 0.1) { // Log roughly 10% of heartbeat responses
                vlog.getEventLogger().trace("HEARTBEAT_RESPONSE", mapOf(
                    "connectionId" to connectionId,
                    "roundTripMs" to roundTripTime,
                    "clientTimestamp" to clientTimestamp
                ))
            }
        } catch (e: Exception) {
            vlog.logVerticleError("HEARTBEAT_PROCESSING", e, mapOf(
                "connectionId" to connectionId
            ))
        }
    }

    /**
     * Start periodic heartbeat for a connection
     */
    private fun startHeartbeat(connectionId: String) {
        // Stop any existing heartbeat for this connection
        stopHeartbeat(connectionId)

        // Run heartbeat every 15 seconds
        val timerId = vertx.setPeriodic(HEARTBEAT_INTERVAL_MS) { _ ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val commandSocket = connectionCache.getCommandSocket(connectionId)
                    if (commandSocket == null || commandSocket.isClosed()) {
                        // Socket is gone, stop heartbeat
                        stopHeartbeat(connectionId)
                        return@launch
                    }

                    // Send heartbeat
                    val heartbeatMessage = JsonObject()
                        .put("type", "heartbeat")
                        .put("timestamp", System.currentTimeMillis())

                    commandSocket.writeTextMessage(heartbeatMessage.encode())

                    // Update last activity
                    connectionStore.updateLastActivity(connectionId)

                    // Only log occasionally to reduce noise
                    if (Math.random() < 0.05) { // Log roughly 5% of heartbeats
                        vlog.getEventLogger().trace("HEARTBEAT_SENT", mapOf(
                            "connectionId" to connectionId
                        ))
                    }
                } catch (e: Exception) {
                    vlog.logVerticleError("HEARTBEAT_SEND", e, mapOf(
                        "connectionId" to connectionId
                    ))

                    // Stop heartbeat if socket appears to be permanently gone
                    if (e.message?.contains("Connection was closed") == true) {
                        stopHeartbeat(connectionId)
                    }
                }
            }
        }

        // Store timer ID for cancellation
        heartbeatTimers[connectionId] = timerId
        vlog.getEventLogger().trace("HEARTBEAT_STARTED", mapOf(
            "connectionId" to connectionId,
            "intervalMs" to HEARTBEAT_INTERVAL_MS
        ))
    }

    /**
     * Stop heartbeat for a connection
     */
    private fun stopHeartbeat(connectionId: String) {
        heartbeatTimers.remove(connectionId)?.let { timerId ->
            vertx.cancelTimer(timerId)
            vlog.getEventLogger().trace("HEARTBEAT_STOPPED", mapOf(
                "connectionId" to connectionId
            ))
        }
    }

    /**
     * Coordinated connection cleanup process
     */
    private suspend fun cleanupConnection(connectionId: String): Boolean {
        val traceId = connectionTraces[connectionId]

        try {
            vlog.getEventLogger().trace("CONNECTION_CLEANUP_STARTED", mapOf(
                "connectionId" to connectionId
            ), traceId)

            // Step 1: Clean up channels
            val channelCleanupResult = cleanupConnectionChannels(connectionId)
            if (!channelCleanupResult) {
                vlog.getEventLogger().trace("CHANNEL_CLEANUP_FAILED", mapOf(
                    "connectionId" to connectionId
                ), traceId)
            }

            // Step 2: Clean up sockets and stop heartbeat
            stopHeartbeat(connectionId)
            val updateSocket = connectionCache.getUpdateSocket(connectionId)
            val socketCleanupResult = cleanupConnectionSockets(connectionId, updateSocket != null)
            if (!socketCleanupResult) {
                vlog.getEventLogger().trace("SOCKET_CLEANUP_FAILED", mapOf(
                    "connectionId" to connectionId
                ), traceId)
            }

            // Step 3: Mark connection as not reconnectable
            try {
                connectionStore.updateReconnectable(connectionId, false)
                vlog.getEventLogger().logStateChange("Connection", "DISCONNECTED", "NOT_RECONNECTABLE",
                    mapOf("connectionId" to connectionId), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError("SET_NOT_RECONNECTABLE", e, mapOf(
                    "connectionId" to connectionId
                ))
            }

            // Step 4: Remove the connection from DB and cache
            try {
                // Try to delete from the database first
                connectionStore.delete(connectionId)
                vlog.getEventLogger().logDbOperation("DELETE", "connections",
                    mapOf("connectionId" to connectionId), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError("DB_CONNECTION_DELETE", e, mapOf(
                    "connectionId" to connectionId
                ))
                // Continue anyway - the connection might already be gone
            }

            // Final cleanup from cache
            try {
                connectionCache.removeConnection(connectionId)
                connectionTraces.remove(connectionId)
                vlog.getEventLogger().trace("CACHE_CONNECTION_REMOVED", mapOf(
                    "connectionId" to connectionId
                ), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError("CACHE_CONNECTION_REMOVE", e, mapOf(
                    "connectionId" to connectionId
                ))
            }

            vlog.getEventLogger().trace("CONNECTION_CLEANUP_COMPLETED", mapOf(
                "connectionId" to connectionId
            ), traceId)

            return true
        } catch (e: Exception) {
            vlog.logVerticleError("CONNECTION_CLEANUP", e, mapOf(
                "connectionId" to connectionId
            ))

            // Do emergency cleanup as a last resort
            try {
                connectionCache.removeCommandSocket(connectionId)
                connectionCache.removeUpdateSocket(connectionId)
                connectionCache.removeConnection(connectionId)
                connectionTraces.remove(connectionId)
                vlog.getEventLogger().trace("EMERGENCY_CLEANUP_COMPLETED", mapOf(
                    "connectionId" to connectionId
                ), traceId)
            } catch (innerEx: Exception) {
                vlog.logVerticleError("EMERGENCY_CLEANUP", innerEx, mapOf(
                    "connectionId" to connectionId
                ))
            }

            return false
        }
    }

    /**
     * Cleans up channel memberships for a connection
     */
    private suspend fun cleanupConnectionChannels(connectionId: String): Boolean {
        val traceId = connectionTraces[connectionId]

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
        val traceId = connectionTraces[connectionId]

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

    /**
     * Handle entity-specific sync request
     */
    private suspend fun handleEntitySync(connectionId: String, entityId: String): Boolean {
        val traceId = connectionTraces[connectionId]

        try {
            vlog.getEventLogger().trace("ENTITY_SYNC_STARTED", mapOf(
                "entityId" to entityId,
                "connectionId" to connectionId
            ), traceId)

            // Check if connection and command socket exist
            if (!connectionCache.hasConnection(connectionId)) {
                vlog.getEventLogger().trace("CONNECTION_NOT_FOUND", mapOf(
                    "connectionId" to connectionId
                ), traceId)
                return false
            }

            val commandSocket = connectionCache.getCommandSocket(connectionId)
            if (commandSocket == null || commandSocket.isClosed()) {
                vlog.getEventLogger().trace("COMMAND_SOCKET_UNAVAILABLE", mapOf(
                    "connectionId" to connectionId
                ), traceId)
                return false
            }

            // Fetch the entity
            val startTime = System.currentTimeMillis()
            vlog.getEventLogger().logDbOperation("FIND", "entities",
                mapOf("entityId" to entityId), traceId)

            val entities = entityStore.findEntitiesByIds(listOf(entityId))

            val queryTime = System.currentTimeMillis() - startTime
            vlog.getEventLogger().logPerformance("ENTITY_QUERY", queryTime,
                mapOf("entityId" to entityId), traceId)

            if (entities.isEmpty()) {
                vlog.getEventLogger().trace("ENTITY_NOT_FOUND", mapOf(
                    "entityId" to entityId
                ), traceId)
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

            vlog.getEventLogger().trace("ENTITY_SYNC_DATA_SENT", mapOf(
                "entityId" to entityId,
                "connectionId" to connectionId
            ), traceId)

            // Update last activity
            connectionStore.updateLastActivity(connectionId)

            vlog.getEventLogger().trace("ENTITY_SYNC_COMPLETED", mapOf(
                "entityId" to entityId,
                "connectionId" to connectionId
            ), traceId)

            return true
        } catch (e: Exception) {
            vlog.logVerticleError("ENTITY_SYNC", e, mapOf(
                "entityId" to entityId,
                "connectionId" to connectionId
            ))
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

                vlog.getEventLogger().trace("SYNC_ERROR_SENT", mapOf(
                    "connectionId" to connectionId,
                    "error" to errorMessage
                ))
            } catch (e: Exception) {
                vlog.logVerticleError("SYNC_ERROR_SEND", e, mapOf(
                    "connectionId" to connectionId
                ))
            }
        }
    }

    override suspend fun stop() {
        vlog.logStartupStep("STOPPING")

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

        // Clean up any active heartbeats
        heartbeatTimers.forEach { (_, timerId) ->
            vertx.cancelTimer(timerId)
        }
        heartbeatTimers.clear()

        vlog.logUndeployment()
    }

    /**
     * Undeploy our dedicated HTTP server verticle
     */
    private suspend fun undeployOwnHttpServer() {
        httpServerVerticleId?.let { id ->
            vlog.logStartupStep("UNDEPLOYING_HTTP_SERVER", mapOf(
                "deploymentId" to id
            ))

            try {
                vertx.undeploy(id).await()
                vlog.logStartupStep("HTTP_SERVER_UNDEPLOYED")
                httpServerVerticleId = null
            } catch (e: Exception) {
                vlog.logVerticleError("UNDEPLOY_HTTP_SERVER", e)
                log.error("Failed to undeploy HTTP server verticle", e)
            }
        }
    }
}