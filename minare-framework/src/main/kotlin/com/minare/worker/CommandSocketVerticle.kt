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
            httpServerPort = config.getInteger("httpPort", 8081)
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
        })

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

    // The rest of the class with all handleCommandSocket, handleReconnection, etc.
    // methods remains the same...

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
}