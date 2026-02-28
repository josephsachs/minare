package com.minare.core.transport.downsocket

import com.google.inject.Inject
import com.minare.application.config.FrameworkConfig
import com.minare.core.Timer
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.adapters.WebsocketProtocol
import com.minare.core.transport.downsocket.pubsub.UpdateBatchCoordinator
import com.minare.core.transport.services.HeartbeatManager
import com.minare.core.transport.upsocket.UpSocketVerticle
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.worker.downsocket.events.UpdateConnectionClosedEvent
import com.minare.worker.downsocket.events.UpdateConnectionClosedEvent.Companion.ADDRESS_CONNECTION_CLOSED
import com.minare.worker.downsocket.events.UpdateConnectionEstablishedEvent
import com.minare.worker.downsocket.events.UpdateConnectionEstablishedEvent.Companion.ADDRESS_CONNECTION_ESTABLISHED
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

class DownSocketVerticle @Inject constructor(
    private val frameworkConfig: FrameworkConfig,
    private val connectionStore: ConnectionStore,
    private val downSocketVerticleCache: DownSocketVerticleCache,
    private val updateConnectionClosedEvent: UpdateConnectionClosedEvent,
    private val updateConnectionEstablishedEvent: UpdateConnectionEstablishedEvent
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(DownSocketVerticle::class.java)
    private lateinit var vlog: VerticleLogger
    private lateinit var eventBusUtils: EventBusUtils
    private lateinit var protocol: WebsocketProtocol
    private lateinit var heartbeatManager: HeartbeatManager
    private lateinit var timer: UpdateTimer

    private var deployedAt: Long = 0

    companion object {
        const val ADDRESS_BROADCAST_CHANNEL = "address.downsocket.broadcast.channel"
    }

    private val httpHost = frameworkConfig.sockets.down.host
    private val httpPort = frameworkConfig.sockets.down.port
    private val defaultTickInterval = frameworkConfig.entity.update.interval

    override suspend fun start() {
        try {
            deployedAt = System.currentTimeMillis()
            vlog = VerticleLogger()
            vlog.setVerticle(this)
            eventBusUtils = vlog.createEventBusUtils()

            protocol = WebsocketProtocol(vertx, vertx.dispatcher(), vlog)
            heartbeatManager = HeartbeatManager(vertx, connectionStore, CoroutineScope(vertx.dispatcher()), protocol.sockets)
            heartbeatManager.setHeartbeatInterval(frameworkConfig.sockets.down.heartbeatInterval)

            protocol.router.setupRoutes("/update") { socket, traceId ->
                handleDownSocket(socket, traceId)
            }

            protocol.router.addHealthEndpoint(
                "/health", "DownSocketVerticle", deploymentID, deployedAt
            ) {
                JsonObject()
                    .put("connections", protocol.sockets.count())
                    .put("heartbeats", heartbeatManager.getMetrics())
                    .put("pendingUpdateQueues", downSocketVerticleCache.connectionPendingUpdates.size)
            }

            registerEventBusConsumers()
            registerBatchedUpdateConsumer()
            registerBroadcastChannelEvent()

            protocol.router.startServer(httpHost, httpPort)

            timer = UpdateTimer()
            timer.start(defaultTickInterval.toInt())

            deploymentID?.let { vlog.logDeployment(it) }
            vlog.logStartupStep("STARTED")
        } catch (e: Exception) {
            log.error("Failed to start DownSocketVerticle", e)
            throw e
        }
    }

    private suspend fun registerEventBusConsumers() {
        updateConnectionEstablishedEvent.register(false)
        updateConnectionClosedEvent.register(false)
    }

    private fun registerBatchedUpdateConsumer() {
        vertx.eventBus().consumer<JsonObject>(UpdateBatchCoordinator.ADDRESS_BATCHED_UPDATES) { message ->
            try {
                forwardUpdateToClients(message.body())
            } catch (e: Exception) {
                log.error("Error processing batched update", e)
            }
        }
    }

    private fun registerBroadcastChannelEvent() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_BROADCAST_CHANNEL) { message, _ ->
            val channelId = message.body().getString("channelId")
            val payload = message.body().getJsonObject("message")

            CoroutineScope(vertx.dispatcher()).launch {
                for (connectionId in downSocketVerticleCache.getConnectionsForChannel(channelId)) {
                    protocol.sockets.send(connectionId, payload.encode())
                }
            }
        }
    }

    private suspend fun handleDownSocket(websocket: ServerWebSocket, traceId: String) {
        websocket.textMessageHandler { message ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val msg = JsonObject(message)

                    if (msg.getString("type") == "heartbeat_response") return@launch

                    val connectionId = msg.getString("connectionId")
                    if (connectionId != null) {
                        associateDownSocket(connectionId, websocket, traceId)
                    } else {
                        protocol.router.sendError(websocket, IllegalArgumentException("No connectionId provided"), null)
                    }
                } catch (e: Exception) {
                    protocol.router.sendError(websocket, e, null)
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

    private suspend fun associateDownSocket(connectionId: String, websocket: ServerWebSocket, traceId: String) {
        try {
            val connection = connectionStore.find(connectionId)

            // Reconnection branch: if there's an existing socket, close it and reuse the connection
            protocol.sockets.close(connectionId)

            val socketId = "down-${UUID.randomUUID()}"

            connectionStore.putDownSocket(connectionId, socketId, deploymentID)
            protocol.sockets.put(connectionId, websocket)

            heartbeatManager.startHeartbeat(socketId, connectionId)
            protocol.router.sendConfirmation(websocket, "down_socket_confirm", connectionId)

            downSocketVerticleCache.connectionPendingUpdates.computeIfAbsent(connectionId) { ConcurrentHashMap() }

            vertx.eventBus().publish(
                ADDRESS_CONNECTION_ESTABLISHED, JsonObject()
                    .put("connectionId", connectionId)
                    .put("socketId", socketId)
                    .put("deploymentId", deploymentID)
            )

            notifyUpsocketFullyConnected(connection.upSocketDeploymentId, connectionId)
        } catch (e: Exception) {
            log.error("Failed to associate down socket for {}", connectionId, e)
            protocol.router.sendError(websocket, e, connectionId)
        }
    }

    private fun notifyUpsocketFullyConnected(upSocketDeploymentId: String?, connectionId: String) {
        if (upSocketDeploymentId == null) {
            log.warn("No upSocketDeploymentId for connection {}, cannot notify fully connected", connectionId)
            return
        }

        vertx.eventBus().send(
            "${UpSocketVerticle.ADDRESS_FULLY_CONNECTED}.${upSocketDeploymentId}",
            JsonObject().put("connectionId", connectionId)
        )
    }

    private fun handleSocketClose(websocket: ServerWebSocket) {
        val connectionId = protocol.sockets.getConnectionId(websocket) ?: return

        try {
            heartbeatManager.stopHeartbeat(connectionId)
            protocol.sockets.remove(connectionId)
            downSocketVerticleCache.invalidateChannelCacheForConnection(connectionId)

            vertx.eventBus().publish(
                ADDRESS_CONNECTION_CLOSED, JsonObject()
                    .put("connectionId", connectionId)
                    .put("socketType", "down")
            )
        } catch (e: Exception) {
            log.error("Error handling down socket close for {}", connectionId, e)
        }
    }

    private fun forwardUpdateToClients(batchedUpdate: JsonObject) {
        for (connectionId in protocol.sockets.allConnectionIds()) {
            sendUpdate(connectionId, batchedUpdate)
        }
    }

    private fun sendUpdate(connectionId: String, update: JsonObject): Boolean {
        return protocol.sockets.send(connectionId, update.encode())
    }

    override suspend fun stop() {
        timer.stop()
        heartbeatManager.stopAll()

        for (connectionId in protocol.sockets.allConnectionIds()) {
            protocol.sockets.close(connectionId)
        }

        protocol.router.stopServer()
    }

    private inner class UpdateTimer : Timer(vertx) {
        override fun tick() {
            try {
                processAndSendUpdates()
            } catch (e: Exception) {
                log.error("Error in tick processing", e)
            }
        }

        private fun processAndSendUpdates() {
            for ((connectionId, updates) in downSocketVerticleCache.connectionPendingUpdates) {
                if (updates.isEmpty()) continue

                val batch = HashMap(updates)
                updates.clear()
                if (batch.isEmpty()) continue

                val updateMessage = JsonObject()
                    .put("type", "update_batch")
                    .put("timestamp", System.currentTimeMillis())
                    .put("updates", JsonObject().apply {
                        batch.forEach { (entityId, update) -> put(entityId, update) }
                    })

                sendUpdate(connectionId, updateMessage)
            }
        }
    }
}