package com.minare.core.transport.downsocket

import com.google.inject.Inject
import com.minare.application.config.FrameworkConfig
import com.minare.core.Timer
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.adapters.WebsocketProtocol
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
    private val channelStore: ChannelStore,
    private val updateConnectionClosedEvent: UpdateConnectionClosedEvent,
    private val updateConnectionEstablishedEvent: UpdateConnectionEstablishedEvent
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(DownSocketVerticle::class.java)

    private lateinit var instanceId: String

    private lateinit var vlog: VerticleLogger
    private lateinit var eventBusUtils: EventBusUtils
    private lateinit var protocol: WebsocketProtocol
    private lateinit var heartbeatManager: HeartbeatManager
    private lateinit var timer: UpdateTimer

    private var deployedAt: Long = 0

    /**
     * Per-instance pending updates. Populated via targeted ADDRESS_SEND_TO_DOWN_CONNECTION
     * messages; flushed to clients by UpdateTimer.
     */
    private val connectionPendingUpdates = ConcurrentHashMap<String, ConcurrentHashMap<String, JsonObject>>()

    companion object {
        const val ADDRESS_BROADCAST_CHANNEL = "address.downsocket.broadcast.channel"
        const val ADDRESS_SEND_TO_DOWN_CONNECTION = "minare.downsocket.send"
    }

    private val httpHost = frameworkConfig.sockets.down.host
    private val httpPort = frameworkConfig.sockets.down.port
    private val defaultTickInterval = frameworkConfig.entity.update.interval

    override suspend fun start() {
        try {
            instanceId = "$deploymentID-${UUID.randomUUID()}"
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
                "/health", "DownSocketVerticle", instanceId, deployedAt
            ) {
                JsonObject()
                    .put("connections", protocol.sockets.count())
                    .put("heartbeats", heartbeatManager.getMetrics())
                    .put("pendingUpdateQueues", connectionPendingUpdates.size)
            }

            registerEventBusConsumers()
            registerSendToDownConnectionConsumer()
            registerBroadcastChannelEvent()

            protocol.router.startServer(httpHost, httpPort)

            timer = UpdateTimer()
            timer.start(defaultTickInterval.toInt())

            vlog.logDeployment(instanceId)
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

    /**
     * Receives targeted entity update messages for connections owned by this instance.
     * Queues the update locally; UpdateTimer flushes it to the client.
     */
    private fun registerSendToDownConnectionConsumer() {
        vertx.eventBus().consumer<JsonObject>("${ADDRESS_SEND_TO_DOWN_CONNECTION}.${instanceId}") { message ->
            try {
                val connectionId = message.body().getString("connectionId") ?: return@consumer
                val entityId = message.body().getString("entityId") ?: return@consumer
                val update = message.body().getJsonObject("update") ?: return@consumer

                val updates = connectionPendingUpdates.computeIfAbsent(connectionId) { ConcurrentHashMap() }
                val existing = updates[entityId]
                if (existing == null || update.getLong("version", 0) > existing.getLong("version", 0)) {
                    updates[entityId] = update
                }
            } catch (e: Exception) {
                log.error("Error queuing update for connection", e)
            }
        }
    }

    private fun registerBroadcastChannelEvent() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_BROADCAST_CHANNEL) { message, _ ->
            val channelId = message.body().getString("channelId")
            val payload = message.body().getJsonObject("message")

            CoroutineScope(vertx.dispatcher()).launch {
                for (connectionId in channelStore.getClientIds(channelId)) {
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
            protocol.sockets.close(connectionId)

            val socketId = "down-${UUID.randomUUID()}"

            val updatedConnection = connectionStore.putDownSocket(connectionId, socketId, instanceId)
            protocol.sockets.put(connectionId, websocket)

            heartbeatManager.startHeartbeat(socketId, connectionId)
            protocol.router.sendConfirmation(websocket, "down_socket_confirm", connectionId)

            connectionPendingUpdates.computeIfAbsent(connectionId) { ConcurrentHashMap() }

            vertx.eventBus().publish(
                ADDRESS_CONNECTION_ESTABLISHED, JsonObject()
                    .put("connectionId", connectionId)
                    .put("socketId", socketId)
                    .put("deploymentId", instanceId)
            )

            notifyUpsocketFullyConnected(updatedConnection.upSocketDeploymentId, connectionId)
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
            connectionPendingUpdates.remove(connectionId)

            vertx.eventBus().publish(
                ADDRESS_CONNECTION_CLOSED, JsonObject()
                    .put("connectionId", connectionId)
                    .put("socketType", "down")
            )
        } catch (e: Exception) {
            log.error("Error handling down socket close for {}", connectionId, e)
        }
    }

    override suspend fun stop() {
        timer.stop()
        heartbeatManager.stopAll()

        for (connectionId in protocol.sockets.allConnectionIds()) {
            protocol.sockets.close(connectionId)
        }

        protocol.router.stopServer()
    }

    /**
     * UpdateTimer flushes pending updates to clients on a fixed interval.
     * Sends one 'update' message per entity, with the entity nested in an updates map
     * to avoid field collisions (e.g. entity "type" vs message "type").
     */
    private inner class UpdateTimer : Timer(vertx) {
        override fun tick() {
            try {
                processAndSendUpdates()
            } catch (e: Exception) {
                log.error("Error in tick processing", e)
            }
        }

        private fun processAndSendUpdates() {
            for ((connectionId, updates) in connectionPendingUpdates) {
                if (updates.isEmpty()) continue

                val batch = HashMap(updates)
                updates.clear()
                if (batch.isEmpty()) continue

                for ((entityId, update) in batch) {
                    val updateMessage = JsonObject()
                        .put("type", "update")
                        .put("timestamp", System.currentTimeMillis())
                        .put("updates", JsonObject().put(entityId, update))

                    protocol.sockets.send(connectionId, updateMessage.encode())
                }
            }
        }
    }
}