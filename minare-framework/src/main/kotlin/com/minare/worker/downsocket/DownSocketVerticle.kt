package com.minare.worker.downsocket

import com.minare.MinareApplication
import com.minare.cache.ConnectionCache
import com.minare.core.Timer
import com.minare.persistence.ConnectionStore
import com.minare.pubsub.UpdateBatchCoordinator
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
import com.minare.worker.upsocket.UpSocketVerticle
import com.minare.worker.downsocket.events.EntityUpdatedEvent
import com.minare.worker.downsocket.events.UpdateConnectionClosedEvent
import com.minare.worker.downsocket.events.UpdateConnectionClosedEvent.Companion.ADDRESS_CONNECTION_CLOSED
import com.minare.worker.downsocket.events.UpdateConnectionEstablishedEvent
import com.minare.worker.downsocket.events.UpdateConnectionEstablishedEvent.Companion.ADDRESS_CONNECTION_ESTABLISHED

/**
 * Verticle responsible for accumulating entity updates and distributing them
 * to clients on a timer.
 *
 * This verticle hosts its own HTTP server for direct WebSocket connections.
 */
class DownSocketVerticle @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache,
    private val downSocketVerticleCache: DownSocketVerticleCache,
    private val entityUpdatedEvent: EntityUpdatedEvent,
    private val updateConnectionClosedEvent: UpdateConnectionClosedEvent,
    private val updateConnectionEstablishedEvent: UpdateConnectionEstablishedEvent
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(DownSocketVerticle::class.java)
    private lateinit var vlog: VerticleLogger
    private lateinit var eventBusUtils: EventBusUtils

    private lateinit var router: Router

    private lateinit var heartbeatManager: HeartbeatManager
    private lateinit var connectionTracker: ConnectionTracker

    private var httpServer: HttpServer? = null

    private lateinit var timer: UpdateTimer
    private val localSockets = HashMap<String, ServerWebSocket>()

    private var deployedAt: Long = 0

    companion object {
        const val ADDRESS_DOWN_SOCKET_INITIALIZED = "minare.down.socketinitialized"
        const val ADDRESS_DOWN_SOCKET_CLOSE = "minare.down.socketclose"
        const val ADDRESS_INITIALIZE = "minare.update.initialize"

        const val CACHE_TTL_MS = 1000L // 10 seconds
        const val HEARTBEAT_INTERVAL_MS = 15000L
        const val DEFAULT_TICK_INTERVAL_MS = 20 // 10 ticks per second

        const val BASE_PATH = "/update"
        const val HTTP_SERVER_HOST = "0.0.0.0"
        const val HTTP_SERVER_PORT = 4226
    }

    override suspend fun start() {
        try {
            deployedAt = System.currentTimeMillis()
            log.info("Starting DownSocketVerticle at {$deployedAt}")

            vlog = VerticleLogger()
            vlog.setVerticle(this)

            eventBusUtils = vlog.createEventBusUtils()
            registerEventBusConsumers()

            connectionTracker = ConnectionTracker("DownSocket", vlog)
            heartbeatManager = HeartbeatManager(vertx, vlog, connectionStore, CoroutineScope(vertx.dispatcher()))
            heartbeatManager.setHeartbeatInterval(UpSocketVerticle.HEARTBEAT_INTERVAL_MS)

            vlog.logStartupStep("STARTING")
            vlog.logConfig(config)

            initializeRouter()

            //timer = UpdateTimer()
            //timer.start(DEFAULT_TICK_INTERVAL_MS)
            log.info("Started Timer at {${System.currentTimeMillis()}}")
            vlog.logStartupStep("UPDATE_TIMER_STARTED", mapOf(
                "intervalMs" to DEFAULT_TICK_INTERVAL_MS
            ))

            // ADDED: Register consumer for batched updates from UpdateBatchCoordinator
            registerBatchedUpdateConsumer()

            vlog.logStartupStep("EVENT_BUS_HANDLERS_REGISTERED")

            deployHttpServer()

            deploymentID?.let {
                vlog.logDeployment(it)
            }

            vlog.logStartupStep("STARTED")
            log.info("DownSocketVerticle started with tick interval: {}ms", DEFAULT_TICK_INTERVAL_MS)
        } catch (e: Exception) {
            vlog.logVerticleError("STARTUP_FAILED", e)
            log.error("Failed to start DownSocketVerticle", e)
            throw e
        }
    }

    /**
     * Register consumer for batched updates from UpdateBatchCoordinator
     */
    private fun registerBatchedUpdateConsumer() {
        vertx.eventBus().consumer<JsonObject>(UpdateBatchCoordinator.ADDRESS_BATCHED_UPDATES) { message ->
            try {
                val batchedUpdate = message.body()
                forwardUpdateToClients(batchedUpdate)
            } catch (e: Exception) {
                vlog.logVerticleError("BATCHED_UPDATE_PROCESSING", e)
            }
        }
        log.info("Registered consumer for batched updates")
    }

    /**
     * Forward a batched update to all connected clients managed by this verticle
     */
    private fun forwardUpdateToClients(batchedUpdate: JsonObject) {
        val connections = connectionTracker.getAllConnectionIds()

        for (connectionId in connections) {
            sendUpdate(connectionId, batchedUpdate)
        }
    }

    /**
     * Initialize the router with down socket routes
     */
    private fun initializeRouter() {
        router = HttpServerUtils.createWebSocketRouter(
            vertx = vertx,
            verticleName = "DownSocketVerticle",
            verticleLogger = vlog,
            wsPath = "/update",
            healthPath = "/health",
            debugPath = "/debug",
            deploymentId = deploymentID,
            deployedAt = deployedAt,
            wsHandler = this::handleDownSocket,
            coroutineContext = vertx.dispatcher(),
            metricsSupplier = {
                JsonObject()
                    .put("connections", connectionTracker.getMetrics())
                    .put("heartbeats", heartbeatManager.getMetrics())
                    .put("pendingUpdateQueues", downSocketVerticleCache.connectionPendingUpdates.size)
                    .put("caches", JsonObject()
                        .put("entityChannel", downSocketVerticleCache.entityChannelCache.size)
                        .put("channelConnection", downSocketVerticleCache.channelConnectionCache.size)
                    )
            }
        )
    }

    /**
     * Deploy a dedicated HTTP server for down sockets
     */
    private suspend fun deployHttpServer() {
        vlog.logStartupStep("DEPLOYING_OWN_HTTP_SERVER")

        try {
            httpServer = HttpServerUtils.createAndStartHttpServer(
                vertx = vertx,
                router = router,
                host = HTTP_SERVER_HOST,
                port = HTTP_SERVER_PORT
            ).await()

            val actualPort = httpServer?.actualPort() ?: HTTP_SERVER_PORT
            vlog.logStartupStep("HTTP_SERVER_DEPLOYED", mapOf(
                "port" to actualPort,
                "host" to HTTP_SERVER_HOST
            ))
        } catch (e: Exception) {
            vlog.logVerticleError("DEPLOY_HTTP_SERVER", e)
            log.error("Failed to deploy HTTP server", e)
            throw e
        }
    }

    /**
     * Register all event bus consumers
     */
    private suspend fun registerEventBusConsumers() {
        //entityUpdatedEvent.register()
        updateConnectionEstablishedEvent.register()
        updateConnectionClosedEvent.register()
    }

    /**
     * Handle a new down socket connection
     */
    private suspend fun handleDownSocket(websocket: ServerWebSocket, traceId: String) {
        log.info("New down socket connection from {}", websocket.remoteAddress())

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

        websocket.accept()
    }

    /**
     * Associate an down socket with a connection ID
     */
    private suspend fun associateUpdateSocket(connectionId: String, websocket: ServerWebSocket, traceId: String) {
        vlog.getEventLogger().trace("ASSOCIATING_SOCKET", mapOf(
            "connectionId" to connectionId
        ), traceId)

        try {
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

            closeExistingUpdateSocket(connectionId, traceId)
            val socketId = "us-${java.util.UUID.randomUUID()}"

            localSockets[connectionId] = websocket
            connectionTracker.registerConnection(connectionId, traceId, websocket)

            connectionStore.putDownSocket(
                connectionId,
                socketId,
                deploymentID // Register the thread context too
            )

            connectionCache.storeDownSocket(connectionId, websocket, connection)
            heartbeatManager.startHeartbeat(socketId, connectionId, websocket)
            WebSocketUtils.sendConfirmation(websocket, "down_socket_confirm", connectionId)

            vlog.getEventLogger().logStateChange("DownSocket", "CONNECTED", "REGISTERED",
                mapOf("connectionId" to connectionId, "socketId" to socketId), traceId)

            downSocketVerticleCache.connectionPendingUpdates.computeIfAbsent(connectionId) { ConcurrentHashMap() }

            vertx.eventBus().publish(
                ADDRESS_CONNECTION_ESTABLISHED, JsonObject()
                    .put("connectionId", connectionId)
                    .put("socketId", socketId)  // Include the socket ID in the event
                    .put("socketType", "down")
                    .put("deploymentId", deploymentID)
            )

            vertx.eventBus().publish(
                MinareApplication.ConnectionEvents.ADDRESS_DOWN_SOCKET_CONNECTED,
                JsonObject()
                    .put("connectionId", connection._id)
                    .put("socketId", socketId)
                    .put("deploymentId", deploymentID)
                    .put("traceId", traceId)
            )
        } catch (e: Exception) {
            vlog.logVerticleError("ASSOCIATE_DOWN_SOCKET", e, mapOf(
                "connectionId" to connectionId
            ))
            WebSocketUtils.sendErrorResponse(websocket, e, connectionId, vlog)
        }
    }

    /**
     * Close any existing down socket for a connection
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
            connectionTracker.handleSocketClosed(websocket)

            localSockets.remove(connectionId)
            connectionCache.removeDownSocket(connectionId)

            vertx.eventBus().publish(
                ADDRESS_CONNECTION_CLOSED, JsonObject()
                    .put("connectionId", connectionId)
                    .put("socketType", "down")
            )

            vlog.getEventLogger().logStateChange("DownSocket", "REGISTERED", "DISCONNECTED",
                mapOf("connectionId" to connectionId), traceId)
        } catch (e: Exception) {
            vlog.logVerticleError("DOWN_SOCKET_CLOSE", e, mapOf(
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
                //TEMPORARY DEBUG
                log.info("Sent update to websocket: {}", update.encode())
                true
            } catch (e: Exception) {
                log.error("Failed to send update to {}", connectionId, e)
                false
            }
        } else {
            log.debug("No down socket found for connection {}", connectionId)
            false
        }
    }

    override suspend fun stop() {
        vlog.logStartupStep("STOPPING")

        timer.stop()

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

        closeAllSockets()

        vlog.logUndeployment()
        log.info("DownSocketVerticle stopped")
    }

    /**
     * Close all websockets
     */
    private fun closeAllSockets() {
        val connectionIds = connectionTracker.getAllConnectionIds().toList()

        for (connectionId in connectionIds) {
            try {
                val socket = connectionTracker.getSocket(connectionId)
                if (socket != null && !socket.isClosed) {
                    socket.close()
                    log.debug("Closed down socket for connection: {}", connectionId)
                }
            } catch (e: Exception) {
                log.error("Error closing socket for connection: {}", connectionId, e)
            }
        }
    }

    /**
     * Timer implementation for update processing.
     */
    private inner class UpdateTimer : Timer(vertx) {
        override fun tick() {
            try {
                processAndSendUpdates()
            } catch (e: Exception) {
                vlog.logVerticleError("TICK_PROCESSING", e)
            }
        }

        /**
         * Process and send all pending updates.
         */
        private fun processAndSendUpdates() {
            val startTime = System.currentTimeMillis()
            var totalConnectionsProcessed = 0
            var totalUpdatesProcessed = 0

            for ((connectionId, updates) in downSocketVerticleCache.connectionPendingUpdates) {
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

                sendUpdate(connectionId, updateMessage)
            }

            val processingTime = System.currentTimeMillis() - startTime

            if (totalUpdatesProcessed > 0 && (
                        totalUpdatesProcessed > 100 ||
                                processingTime > getIntervalMs() / 2 ||
                                counter % 100 == 0L
                        )) {
                vlog.logVerticlePerformance("TICK_UPDATE", processingTime, mapOf(
                    "updatesProcessed" to totalUpdatesProcessed,
                    "connectionsProcessed" to totalConnectionsProcessed
                ))
            }
        }
    }
}