package com.minare.worker.update

import com.minare.MinareApplication
import com.minare.cache.ConnectionCache
import com.minare.core.FrameController
import com.minare.persistence.ChannelStore
import com.minare.persistence.ConnectionStore
import com.minare.persistence.ContextStore
import com.minare.utils.*
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
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import com.minare.worker.command.CommandVerticle
import com.minare.worker.update.UpdateVerticleCache
import com.minare.worker.update.events.EntityUpdatedEvent
import com.minare.worker.update.events.UpdateConnectionClosedEvent
import com.minare.worker.update.events.UpdateConnectionClosedEvent.Companion.ADDRESS_CONNECTION_CLOSED
import com.minare.worker.update.events.UpdateConnectionEstablishedEvent
import com.minare.worker.update.events.UpdateConnectionEstablishedEvent.Companion.ADDRESS_CONNECTION_ESTABLISHED

/**
 * Verticle responsible for accumulating entity updates and distributing them
 * to clients on a frame-based schedule.
 *
 * This verticle hosts its own HTTP server for direct WebSocket connections.
 */
class UpdateVerticle @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache,
    private val updateVerticleCache: UpdateVerticleCache,
    private val entityUpdatedEvent: EntityUpdatedEvent,
    private val updateConnectionClosedEvent: UpdateConnectionClosedEvent,
    private val updateConnectionEstablishedEvent: UpdateConnectionEstablishedEvent
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(UpdateVerticle::class.java)
    private lateinit var vlog: VerticleLogger
    private lateinit var eventBusUtils: EventBusUtils

    private lateinit var router: Router

    private lateinit var heartbeatManager: HeartbeatManager
    private lateinit var connectionTracker: ConnectionTracker

    private var httpServer: HttpServer? = null

    private lateinit var frameController: UpdateFrameController

    private var deployedAt: Long = 0
    private var useOwnHttpServer: Boolean = true
    private var httpServerPort: Int = 4226
    private var httpServerHost: String = "0.0.0.0"

    companion object {
        const val ADDRESS_UPDATE_SOCKET_INITIALIZED = "minare.update.socket.initialized"
        const val ADDRESS_UPDATE_SOCKET_CLOSE = "minare.update.socket.close"
        const val ADDRESS_INITIALIZE = "minare.update.initialize"

        const val CACHE_TTL_MS = 10000L // 10 seconds
        const val HEARTBEAT_INTERVAL_MS = 15000L
        const val DEFAULT_FRAME_INTERVAL_MS = 100 // 10 frames per second
    }

    override suspend fun start() {
        try {
            deployedAt = System.currentTimeMillis()
            log.info("Starting UpdateVerticle at {$deployedAt}")

            vlog = VerticleLogger()
            vlog.setVerticle(this)

            eventBusUtils = vlog.createEventBusUtils()

            connectionTracker = ConnectionTracker("UpdateSocket", vlog)
            heartbeatManager = HeartbeatManager(vertx, vlog, connectionStore, CoroutineScope(vertx.dispatcher()))
            heartbeatManager.setHeartbeatInterval(CommandVerticle.HEARTBEAT_INTERVAL_MS)

            vlog.logStartupStep("STARTING")
            vlog.logConfig(config)

            useOwnHttpServer = config.getBoolean("useOwnHttpServer", true)
            httpServerPort = config.getInteger("httpPort", 4226)
            httpServerHost = config.getString("httpHost", "0.0.0.0")
            initializeRouter()

            frameController = UpdateFrameController()
            frameController.start(DEFAULT_FRAME_INTERVAL_MS)
            log.info("Started FrameController at {${System.currentTimeMillis()}}")
            vlog.logStartupStep("FRAME_CONTROLLER_STARTED", mapOf(
                "frameInterval" to DEFAULT_FRAME_INTERVAL_MS
            ))

            registerEventBusConsumers()
            vlog.logStartupStep("EVENT_BUS_HANDLERS_REGISTERED")

            if (useOwnHttpServer) {
                deployHttpServer()
            }

            // Save deployment ID
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
     * Initialize the router with update socket routes
     */
    private fun initializeRouter() {
        router = HttpServerUtils.createWebSocketRouter(
            vertx = vertx,
            verticleName = "UpdateVerticle",
            verticleLogger = vlog,
            wsPath = "/update",
            healthPath = "/health",
            debugPath = "/debug",
            deploymentId = deploymentID,
            deployedAt = deployedAt,
            wsHandler = this::handleUpdateSocket,
            coroutineContext = vertx.dispatcher(),
            metricsSupplier = {
                JsonObject()
                    .put("connections", connectionTracker.getMetrics())
                    .put("heartbeats", heartbeatManager.getMetrics())
                    .put("pendingUpdateQueues", updateVerticleCache.connectionPendingUpdates.size)
                    .put("caches", JsonObject()
                        .put("entityChannel", updateVerticleCache.entityChannelCache.size)
                        .put("channelConnection", updateVerticleCache.channelConnectionCache.size)
                    )
            }
        )
    }

    /**
     * Deploy a dedicated HTTP server for update sockets
     */
    private suspend fun deployHttpServer() {
        vlog.logStartupStep("DEPLOYING_OWN_HTTP_SERVER")

        try {
            httpServer = HttpServerUtils.createAndStartHttpServer(
                vertx = vertx,
                router = router,
                host = httpServerHost,
                port = httpServerPort
            ).await()

            val actualPort = httpServer?.actualPort() ?: httpServerPort
            vlog.logStartupStep("HTTP_SERVER_DEPLOYED", mapOf(
                "port" to actualPort,
                "host" to httpServerHost
            ))
        } catch (e: Exception) {
            vlog.logVerticleError("DEPLOY_HTTP_SERVER", e)
            log.error("Failed to deploy HTTP server", e)

            useOwnHttpServer = false
            throw e
        }
    }

    /**
     * Register all event bus consumers
     */
    private suspend fun registerEventBusConsumers() {
        entityUpdatedEvent.register()
        updateConnectionEstablishedEvent.register()
        updateConnectionClosedEvent.register()
    }

    /**
     * Handle a new update socket connection
     */
    private suspend fun handleUpdateSocket(websocket: ServerWebSocket, traceId: String) {
        log.info("New update socket connection from {}", websocket.remoteAddress())

        websocket.textMessageHandler { message ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val msg = JsonObject(message)
                    val connectionId = msg.getString("connectionId")

                    if (connectionId != null) {
                        associateUpdateSocket(connectionId, websocket, traceId)
                    } else {
                        WebSocketUtils.sendErrorResponse(
                            websocket,
                            IllegalArgumentException("No connectionId provided"),
                            null,
                            vlog
                        )
                    }
                } catch (e: Exception) {
                    WebSocketUtils.sendErrorResponse(websocket, e, null, vlog)
                }
            }
        }

        websocket.closeHandler {
            CoroutineScope(vertx.dispatcher()).launch {
                handleSocketClose(websocket)
            }
        }
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
                WebSocketUtils.sendErrorResponse(
                    websocket,
                    IllegalArgumentException("Connection not found: $connectionId"),
                    null,
                    vlog
                )
                return
            }

            // Close any existing socket for this connection
            closeExistingUpdateSocket(connectionId, traceId)

            // Register the new socket
            connectionTracker.registerConnection(connectionId, traceId, websocket)
            connectionCache.storeUpdateSocket(connectionId, websocket, connection)

            // Send confirmation
            WebSocketUtils.sendConfirmation(websocket, "update_socket_confirm", connectionId)

            vlog.getEventLogger().logStateChange("UpdateSocket", "CONNECTED", "REGISTERED",
                mapOf("connectionId" to connectionId), traceId)

            // Initialize pending updates for this connection if not already present
            updateVerticleCache.connectionPendingUpdates.computeIfAbsent(connectionId) { ConcurrentHashMap() }

            // Notify about connection established
            vertx.eventBus().publish(
                ADDRESS_CONNECTION_ESTABLISHED, JsonObject()
                .put("connectionId", connectionId)
                .put("socketType", "update")
            )

            vlog.getEventLogger().trace("UPDATE_SOCKET_ASSOCIATED", mapOf(
                "connectionId" to connectionId,
                "socketId" to websocket.textHandlerID()
            ), traceId)

            // Publish for the Update Socket
            vertx.eventBus().publish(
                MinareApplication.ConnectionEvents.ADDRESS_UPDATE_SOCKET_CONNECTED,
                JsonObject()
                    .put("connectionId", connection._id)
                    .put("traceId", traceId)
            )
        } catch (e: Exception) {
            vlog.logVerticleError("ASSOCIATE_UPDATE_SOCKET", e, mapOf(
                "connectionId" to connectionId
            ))
            WebSocketUtils.sendErrorResponse(websocket, e, connectionId, vlog)
        }
    }

    /**
     * Close any existing update socket for a connection
     */
    private fun closeExistingUpdateSocket(connectionId: String, traceId: String) {
        val existingSocket = connectionTracker.getSocket(connectionId) ?: return

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

    /**
     * Handle a socket being closed
     */
    private suspend fun handleSocketClose(websocket: ServerWebSocket) {
        val connectionId = connectionTracker.getConnectionId(websocket) ?: return
        val traceId = connectionTracker.getTraceId(connectionId)

        try {
            // Remove from tracking
            connectionTracker.handleSocketClosed(websocket)

            // Remove from connection cache
            connectionCache.removeUpdateSocket(connectionId)

            // Notify about connection closed
            vertx.eventBus().publish(
                ADDRESS_CONNECTION_CLOSED, JsonObject()
                .put("connectionId", connectionId)
                .put("socketType", "update")
            )

            vlog.getEventLogger().logStateChange("UpdateSocket", "REGISTERED", "DISCONNECTED",
                mapOf("connectionId" to connectionId), traceId)
        } catch (e: Exception) {
            vlog.logVerticleError("UPDATE_SOCKET_CLOSE", e, mapOf(
                "connectionId" to connectionId
            ))
        }
    }



    /**
     * Send an update directly to a client's WebSocket
     */
    private fun sendUpdate(connectionId: String, update: JsonObject): Boolean {
        val socket = connectionTracker.getSocket(connectionId)

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

        // Clean up all websockets
        closeAllSockets()

        vlog.logUndeployment()
        log.info("UpdateVerticle stopped")
    }

    /**
     * Close all websockets
     */
    private fun closeAllSockets() {
        val connectionIds = connectionTracker.getAllConnectionIds().toList()

        for (connectionId in connectionIds) {
            try {
                val socket = connectionTracker.getSocket(connectionId)
                if (socket != null && !socket.isClosed()) {
                    socket.close()
                    log.debug("Closed update socket for connection: {}", connectionId)
                }
            } catch (e: Exception) {
                log.error("Error closing socket for connection: {}", connectionId, e)
            }
        }
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
            for ((connectionId, updates) in updateVerticleCache.connectionPendingUpdates) {
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

                // Send directly to the client's WebSocket
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