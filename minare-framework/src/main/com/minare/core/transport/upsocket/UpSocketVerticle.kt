package com.minare.worker.upsocket

import com.minare.controller.MessageController
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.utils.HeartbeatManager
import com.minare.core.transport.downsocket.services.ConnectionTracker
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType
import com.minare.utils.HttpServerUtils
import com.minare.utils.WebSocketUtils
import com.minare.worker.upsocket.events.*
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
import com.minare.worker.upsocket.handlers.CloseHandler
import com.minare.worker.upsocket.handlers.ReconnectionHandler

/**
 * Verticle responsible for managing up socket connections and handling
 * socket lifecycle events. Creates and manages its own router for WebSocket endpoints.
 */
class UpSocketVerticle @Inject constructor(
    private val vlog: VerticleLogger,
    private val heartbeatManager: HeartbeatManager,
    private val connectionTracker: ConnectionTracker,
    private val reconnectionHandler: ReconnectionHandler,
    private val messageController: MessageController,
    private val closeHandler: CloseHandler,
    private val connectionLifecycle: ConnectionLifecycle,
    private val channelCleanupEvent: ChannelCleanupEvent,
    private val upSocketGetRouterEvent: UpSocketGetRouterEvent,
    private val upSocketCleanupEvent: UpSocketCleanupEvent,
    private val upSocketInitEvent: UpSocketInitEvent,
    private val connectionCleanupEvent: ConnectionCleanupEvent,
    private val entitySyncEvent: EntitySyncEvent,
    private val debug: DebugLogger
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(UpSocketVerticle::class.java)
    // Silence is golden
    private val debugTraceLogs: Boolean = true

    private var deployedAt: Long = 0
    private var httpServer: HttpServer? = null
    lateinit var router: Router

    companion object {
        const val ADDRESS_UP_SOCKET_INITIALIZE = "minare.up.socket.initialize"
        const val ADDRESS_SOCKET_CLEANUP = "minare.socket.cleanup"
        const val ADDRESS_CONNECTION_CLEANUP = "minare.connection.cleanup"
        const val ADDRESS_ENTITY_SYNC = "minare.entity.sync"
        const val ADDRESS_GET_ROUTER = "minare.up.socket.get.router"

        const val HANDSHAKE_TIMEOUT_MS = 3000L
        const val HEARTBEAT_INTERVAL_MS = 15000L

        const val BASE_PATH = "/command"
        const val HTTP_SERVER_PORT = 4225
        const val HTTP_SERVER_HOST = "0.0.0.0"
    }

    override suspend fun start() {
        vlog.setVerticle(this)

        deployedAt = System.currentTimeMillis()
        debug.log(DebugType.UPSOCKET_STARTUP, listOf(deployedAt, vlog, config))

        router = Router.router(vertx)
        debug.log(DebugType.UPSOCKET_ROUTER_CREATED, listOf(vlog))
        initializeRouter()

        registerEventBusConsumers()
        deployHttpServer()

        // Save deployment ID
        // TODO: If this is the deployment ID for the worker, it should be set in MinareApplication instead
        deploymentID?.let {
            vlog.logDeployment(it)
        }

        vlog.logStartupStep("STARTED")
    }

    /**
     * Initialize the router with up socket routes
     */
    private fun initializeRouter() {
        debug.log(DebugType.UPSOCKET_INITIALIZING_ROUTER, listOf(vlog))

        HttpServerUtils.addDebugEndpoint(router, "/ws-debug", "UpSocketVerticle")

        debug.log(DebugType.UPSOCKET_SETTING_UP_ROUTE_HANDLER, listOf(BASE_PATH))

        router.route(BASE_PATH).handler { context ->
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
            verticleName = "UpVerticle",
            deploymentId = deploymentID,
            deployedAt = deployedAt
        ) {
            JsonObject()
                .put("connections", connectionTracker.getMetrics())
                .put("heartbeats", heartbeatManager.getMetrics())
        }

        debug.log(DebugType.UPSOCKET_ROUTER_INITIALIZED, listOf(BASE_PATH, vlog))
    }

    /**
     * Register all event bus consumers
     */
    private suspend fun registerEventBusConsumers() {
        upSocketInitEvent.register(debugTraceLogs)
        upSocketGetRouterEvent.register(router)
        channelCleanupEvent.register(debugTraceLogs)
        upSocketCleanupEvent.register(debugTraceLogs)
        connectionCleanupEvent.register(debugTraceLogs)
        entitySyncEvent.register(debugTraceLogs)
    }

    /**
     * Handle a new up socket connection
     */
    private suspend fun handleMessage(websocket: ServerWebSocket, traceId: String) {
        var handshakeCompleted = false

        debug.log(DebugType.UPSOCKET_NEW_WEBSOCKET_CONNECTION, listOf(websocket.remoteAddress()))

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
                            reconnectionHandler.handle(websocket, connectionId, deploymentID, traceId)
                        } else {
                            // For regular messages after handshake, use the standard handler
                            val msg = JsonObject(message)
                            messageController.handleUpsocket(connectionTracker, websocket, msg)
                        }
                    } else {
                        // Regular message handling after handshake is complete
                        val msg = JsonObject(message)
                        messageController.handleUpsocket(connectionTracker, websocket, msg)
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

            debug.log(DebugType.UPSOCKET_WEBSOCKET_CLOSED, listOf(vlog, websocket.textHandlerID(), connectionId, traceId))
        }

        websocket.accept()

        if (debugTraceLogs) {
            vlog.getEventLogger().trace(
                "WEBSOCKET_ACCEPTED", mapOf(
                    "socketId" to websocket.textHandlerID()
                ), traceId
            )
        }

        // If no reconnection message is received, create a new connection
        vertx.setTimer(HANDSHAKE_TIMEOUT_MS) {
            if (!handshakeCompleted) {
                debug.log(DebugType.UPSOCKET_CONNECTION_TIMEOUT, listOf(vlog, websocket.textHandlerID(), HANDSHAKE_TIMEOUT_MS, traceId))

                handshakeCompleted = true
                CoroutineScope(vertx.dispatcher()).launch {
                    try {
                        connectionLifecycle.initiateConnection(websocket, deploymentID, traceId)
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
     * Deploy a dedicated HTTP server for up sockets
     */
    private suspend fun deployHttpServer() {
        debug.log(DebugType.UPSOCKET_DEPLOYING_HTTP_SERVER, listOf(vlog))

        try {
            httpServer = HttpServerUtils.createAndStartHttpServer(
                vertx = vertx,
                router = router,
                host = HTTP_SERVER_HOST,
                port = HTTP_SERVER_PORT
            ).await()

            val actualPort = httpServer?.actualPort() ?: HTTP_SERVER_PORT

            debug.log(DebugType.UPSOCKET_HTTP_SERVER_DEPLOYED, listOf(vlog, actualPort, HTTP_SERVER_HOST))

        } catch (e: Exception) {
            vlog.logVerticleError("DEPLOY_HTTP_SERVER", e)
            log.error("Failed to deploy HTTP server", e)
        }
    }

    override suspend fun stop() {
        debug.log(DebugType.UPSOCKET_HTTP_SERVER_STOPPING, listOf(vlog))

        heartbeatManager.stopAll()

        if (httpServer != null) {
            try {
                httpServer!!.close().await()
            } catch (e: Exception) {
                log.error("Error closing HTTP server", e)
            }
        }
    }
}