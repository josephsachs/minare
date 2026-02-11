package com.minare.core.transport.upsocket

import com.minare.application.config.FrameworkConfig
import com.minare.controller.MessageController
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.transport.services.HeartbeatManager
import com.minare.core.transport.downsocket.services.ConnectionTracker
import com.minare.core.transport.upsocket.events.EntitySyncEvent
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType
import com.minare.core.transport.services.HttpServerUtils
import com.minare.core.transport.services.WebSocketUtils
import com.minare.worker.upsocket.ConnectionLifecycle
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
import com.google.inject.Inject
import com.minare.worker.upsocket.handlers.CloseHandler
import com.minare.worker.upsocket.handlers.ReconnectionHandler

/**
 * Verticle responsible for managing up socket connections and handling
 * socket lifecycle events. Creates and manages its own router for WebSocket endpoints.
 */
class UpSocketVerticle @Inject constructor(
    private val vlog: VerticleLogger,
    private val heartbeatManager: HeartbeatManager,
    private val frameworkConfig: FrameworkConfig,
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
    // Looks like we still have some debug logs that need to go into the configurator
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
    }

    private val basePath = frameworkConfig.sockets.up.basePath
    private val httpPort = frameworkConfig.sockets.up.port
    private val httpHost = frameworkConfig.sockets.up.host
    private val handshakeTimeoutMs = frameworkConfig.sockets.up.handshakeTimeout

    override suspend fun start() {
        vlog.setVerticle(this)

        deployedAt = System.currentTimeMillis()
        debug.log(DebugType.UPSOCKET_STARTUP, listOf(deployedAt, vlog, config))

        router = Router.router(vertx)
        debug.log(DebugType.UPSOCKET_ROUTER_CREATED, listOf(vlog))
        initializeRouter()

        registerEventBusConsumers()
        deployHttpServer()

        deploymentID.let {
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

        debug.log(DebugType.UPSOCKET_SETTING_UP_ROUTE_HANDLER, listOf(basePath))

        router.route(basePath).handler { context ->
            WebSocketUtils.handleWebSocketUpgrade(
                context,
                vertx.dispatcher(),
                basePath,
                vlog
            ) { socket, traceId ->
                handleMessage(socket, traceId)
            }
        }

        HttpServerUtils.addHealthEndpoint(
            router = router,
            path = "$basePath/health",
            verticleName = "UpVerticle",
            deploymentId = deploymentID,
            deployedAt = deployedAt
        ) {
            JsonObject()
                .put("connections", connectionTracker.getMetrics())
                .put("heartbeats", heartbeatManager.getMetrics())
        }

        debug.log(DebugType.UPSOCKET_ROUTER_INITIALIZED, listOf(basePath, vlog))
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
        entitySyncEvent.register()
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
                    val msg = JsonObject(message)

                    if (!handshakeCompleted) {
                        if (msg.containsKey("reconnect") && msg.containsKey("connectionId")) {
                            val connectionId = msg.getString("connectionId")

                            handshakeCompleted = true
                            reconnectionHandler.handle(websocket, connectionId, deploymentID, traceId)
                        }
                    }

                    if (frameworkConfig.sockets.up.ack) {
                        websocket.writeTextMessage(JsonObject().put("type", "ack").toString())
                    }

                    messageController.handleUpsocket(connectionTracker, websocket, msg)
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
                debug.log(DebugType.UPSOCKET_CLOSED_CONNECTION_MISSING_ID, listOf(websocket.textHandlerID()))
            }

            debug.log(DebugType.UPSOCKET_WEBSOCKET_CLOSED, listOf(vlog, websocket.textHandlerID(), connectionId, traceId))
        }

        websocket.accept()
        debug.log(DebugType.UPSOCKET_SOCKET_ACCEPTED, listOf(vlog, websocket.textHandlerID(), traceId))

        vertx.setTimer(handshakeTimeoutMs) {
            if (!handshakeCompleted) {
                debug.log(DebugType.UPSOCKET_CONNECTION_TIMEOUT, listOf(vlog, websocket.textHandlerID(), handshakeTimeoutMs, traceId))

                handshakeCompleted = true
                CoroutineScope(vertx.dispatcher()).launch {
                    try {
                        connectionLifecycle.initiateConnection(websocket, deploymentID, traceId)
                    } catch (e: Exception) {
                        debug.log(DebugType.UPSOCKET_INITIATE_CONNECTION_ERROR, listOf(vlog, e, websocket.textHandlerID()))

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
                host = httpHost,
                port = httpPort
            ).await()

            val actualPort = httpServer?.actualPort() ?: httpPort

            debug.log(DebugType.UPSOCKET_HTTP_SERVER_DEPLOYED, listOf(vlog, actualPort, httpHost))

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