package com.minare.worker.command

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
import com.minare.worker.command.handlers.CloseHandler
import com.minare.worker.command.handlers.MessageHandler
import com.minare.worker.command.handlers.ReconnectionHandler

/**
 * Verticle responsible for managing command socket connections and handling
 * socket lifecycle events. Creates and manages its own router for WebSocket endpoints.
 */
class CommandVerticle @Inject constructor(
    private val vlog: VerticleLogger,
    private val heartbeatManager: HeartbeatManager,
    private val connectionTracker: ConnectionTracker,
    private val reconnectionHandler: ReconnectionHandler,
    private val messageHandler: MessageHandler,
    private val closeHandler: CloseHandler,
    private val connectionLifecycle: ConnectionLifecycle
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(CommandVerticle::class.java)
    private var deployedAt: Long = 0
    private var httpServer: HttpServer? = null
    lateinit var router: Router

    companion object {
        const val ADDRESS_COMMAND_SOCKET_INITIALIZE = "minare.command.socket.initialize"
        const val ADDRESS_CONNECTION_CLEANUP = "minare.connection.cleanup"

        const val ADDRESS_SOCKET_CLEANUP = "minare.socket.cleanup"
        const val ADDRESS_ENTITY_SYNC = "minare.entity.sync"
        const val ADDRESS_GET_ROUTER = "minare.command.socket.get.router"

        const val HANDSHAKE_TIMEOUT_MS = 3000L
        const val HEARTBEAT_INTERVAL_MS = 15000L

        const val BASE_PATH = "/command"
        const val HTTP_SERVER_PORT = 4225
        const val HTTP_SERVER_HOST = "0.0.0.0"
    }

    override suspend fun start() {
        deployedAt = System.currentTimeMillis()
        log.info("Starting CommandSocketVerticle at {$deployedAt}")

        vlog.setVerticle(this)

        vlog.logStartupStep("STARTING")
        vlog.logConfig(config)

        router = Router.router(vertx)
        vlog.logStartupStep("ROUTER_CREATED")
        initializeRouter()

        registerEventBusConsumers()
        vlog.logStartupStep("EVENT_BUS_HANDLERS_REGISTERED")

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

        HttpServerUtils.addDebugEndpoint(router, "/ws-debug", "CommandSocketVerticle")
        log.info("Setting up websocket route handler at path: {}", BASE_PATH)

        router.route("$BASE_PATH").handler { context ->
            WebSocketUtils.handleWebSocketUpgrade(
                context,
                vertx.dispatcher(),
                BASE_PATH,
                vlog
            ) { socket, traceId ->
                handleMessage(socket, traceId)
            }
        }

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
     * Register all event bus consumers
     */
    private suspend fun registerEventBusConsumers() {

    }

    /**
     * Handle a new command socket connection
     */
    private suspend fun handleMessage(websocket: ServerWebSocket, traceId: String) {
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
                            reconnectionHandler.handle(websocket, connectionId, traceId)
                        } else {
                            // For regular messages after handshake, use the standard handler
                            val msg = JsonObject(message)
                            messageHandler.handle(websocket, msg)
                        }
                    } else {
                        // Regular message handling after handshake is complete
                        val msg = JsonObject(message)
                        messageHandler.handle(websocket, msg)
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
            val connectionId = connectionTracker.getConnectionId(websocket)

            if (connectionId != null) {
                CoroutineScope(vertx.dispatcher()).launch {
                    closeHandler.handle(websocket, connectionId)
                }
            } else {
                log.warn("Tried to close websocket ${websocket.textHandlerID()} with no connection")
            }

            vlog.getEventLogger().trace(
                "WEBSOCKET_CLOSED", mapOf(
                    "socketId" to websocket.textHandlerID(),
                    "connectionId" to connectionId
                ), traceId
            )
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
                        connectionLifecycle.initiateConnection(websocket, traceId)
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
     * Deploy a dedicated HTTP server for command sockets
     */
    suspend fun deployOwnHttpServer() {
        vlog.logStartupStep("DEPLOYING_OWN_HTTP_SERVER")

        try {
            // Use the utility to create and start the server
            httpServer = HttpServerUtils.createAndStartHttpServer(
                vertx = vertx,
                router = router,
                host = HTTP_SERVER_HOST,
                port = HTTP_SERVER_PORT
            ).await()

            val actualPort = httpServer?.actualPort() ?: HTTP_SERVER_PORT
            vlog.logStartupStep(
                "HTTP_SERVER_DEPLOYED", mapOf(
                    "port" to actualPort,
                    "host" to HTTP_SERVER_HOST
                )
            )
        } catch (e: Exception) {
            vlog.logVerticleError("DEPLOY_HTTP_SERVER", e)
            log.error("Failed to deploy HTTP server", e)
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