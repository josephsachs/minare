package com.minare.core.transport.upsocket

import com.google.inject.Inject
import com.minare.application.config.FrameworkConfig
import com.minare.controller.ConnectionController
import com.minare.controller.MessageController
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.adapters.WebsocketProtocol
import com.minare.core.transport.services.HeartbeatManager
import com.minare.core.transport.upsocket.events.EntitySyncEvent
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.worker.upsocket.events.ConnectionCleanupEvent
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.UUID

class UpSocketVerticle @Inject constructor(
    private val vlog: VerticleLogger,
    private val frameworkConfig: FrameworkConfig,
    private val connectionStore: ConnectionStore,
    private val connectionController: ConnectionController,
    private val messageController: MessageController,
    private val connectionCleanupEvent: ConnectionCleanupEvent,
    private val entitySyncEvent: EntitySyncEvent,
    private val debug: DebugLogger
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(UpSocketVerticle::class.java)

    private lateinit var instanceId: String

    private lateinit var protocol: WebsocketProtocol
    private lateinit var heartbeatManager: HeartbeatManager

    private var deployedAt: Long = 0

    companion object {
        const val ADDRESS_CONNECTION_CLEANUP = "minare.connection.cleanup"
        const val ADDRESS_ENTITY_SYNC = "minare.entity.sync"
        const val ADDRESS_FULLY_CONNECTED = "minare.connection.fully_connected"
        const val ADDRESS_SEND_TO_CONNECTION = "minare.upsocket.send"
    }

    private val basePath = frameworkConfig.sockets.up.basePath
    private val httpPort = frameworkConfig.sockets.up.port
    private val httpHost = frameworkConfig.sockets.up.host
    private val handshakeTimeoutMs = frameworkConfig.sockets.up.handshakeTimeout

    override suspend fun start() {
        instanceId = "$deploymentID-${UUID.randomUUID()}"
        deployedAt = System.currentTimeMillis()
        vlog.setVerticle(this)

        protocol = WebsocketProtocol(vertx, vertx.dispatcher(), vlog)
        heartbeatManager = HeartbeatManager(vertx, connectionStore, CoroutineScope(vertx.dispatcher()), protocol.sockets)

        heartbeatManager.setHeartbeatInterval(frameworkConfig.sockets.up.heartbeatInterval)

        protocol.router.setupRoutes(basePath) { socket, traceId ->
            handleConnection(socket, traceId)
        }

        protocol.router.addHealthEndpoint(
            "$basePath/health", "UpSocketVerticle", instanceId, deployedAt
        ) {
            JsonObject()
                .put("connections", protocol.sockets.count())
                .put("heartbeats", heartbeatManager.getMetrics())
        }

        registerEventBusConsumers()
        registerFullyConnectedConsumer()
        registerSendToConnectionConsumer()

        protocol.router.startServer(httpHost, httpPort)

        vlog.logDeployment(instanceId)
        vlog.logStartupStep("STARTED")
    }

    private suspend fun registerEventBusConsumers() {
        connectionCleanupEvent.register(false)
        entitySyncEvent.register()
    }

    /**
     * Receives notification from DownSocketVerticle that a connection is fully established.
     * Targeted by instanceId so only the correct upsocket instance handles it.
     */
    private fun registerFullyConnectedConsumer() {
        vertx.eventBus().consumer<JsonObject>("${ADDRESS_FULLY_CONNECTED}.${instanceId}") { message ->
            val connectionId = message.body().getString("connectionId")
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val connection = connectionStore.find(connectionId)
                    connectionController.onClientFullyConnected(connection)
                } catch (e: Exception) {
                    log.error("Error handling fully connected for {}", connectionId, e)
                }
            }
        }
    }

    /**
     * Receives targeted messages from controllers that need to send to a specific connection's socket.
     * Targeted by instanceId so only the instance holding the socket handles it.
     */
    private fun registerSendToConnectionConsumer() {
        vertx.eventBus().consumer<JsonObject>("${ADDRESS_SEND_TO_CONNECTION}.${instanceId}") { message ->
            val connectionId = message.body().getString("connectionId")
            val payload = message.body().getJsonObject("message")
            if (connectionId != null && payload != null) {
                protocol.sockets.send(connectionId, payload.encode())
            }
        }
    }

    private suspend fun handleConnection(websocket: ServerWebSocket, traceId: String) {
        var handshakeCompleted = false

        websocket.textMessageHandler { message ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val msg = JsonObject(message)

                    if (!handshakeCompleted && msg.containsKey("reconnect") && msg.containsKey("connectionId")) {
                        handshakeCompleted = true
                        // Upsocket does not support reconnection; issue new connection
                        initiateConnection(websocket, traceId)
                        return@launch
                    }

                    if (frameworkConfig.sockets.up.ack) {
                        protocol.router.sendMessage(websocket, JsonObject()
                            .put("type", "ack")
                            .put("traceId", messageController.getTraceId())
                        )
                    }

                    messageController.handleUpsocket(protocol.sockets.getConnectionId(websocket), msg)
                } catch (e: Exception) {
                    protocol.router.sendError(websocket, e, protocol.sockets.getConnectionId(websocket))
                }
            }
        }

        websocket.closeHandler {
            CoroutineScope(vertx.dispatcher()).launch {
                handleClose(websocket)
            }
        }

        websocket.accept()

        vertx.setTimer(handshakeTimeoutMs) {
            if (!handshakeCompleted) {
                handshakeCompleted = true
                CoroutineScope(vertx.dispatcher()).launch {
                    initiateConnection(websocket, traceId)
                }
            }
        }
    }

    private suspend fun initiateConnection(websocket: ServerWebSocket, traceId: String) {
        try {
            val connection = connectionStore.create()
            val socketId = "up-${UUID.randomUUID()}"

            val updatedConnection = connectionStore.putUpSocket(connection.id, socketId, instanceId)
            protocol.sockets.put(connection.id, websocket)

            protocol.router.sendConfirmation(websocket, "connection_confirm", connection.id)
            heartbeatManager.startHeartbeat(socketId, connection.id)
        } catch (e: Exception) {
            log.error("Failed to initiate connection", e)
            protocol.router.sendError(websocket, e, null)
        }
    }

    private suspend fun handleClose(websocket: ServerWebSocket) {
        val connectionId = protocol.sockets.getConnectionId(websocket) ?: return

        try {
            heartbeatManager.stopHeartbeat(connectionId)
            connectionStore.updateReconnectable(connectionId, false)
            protocol.sockets.remove(connectionId)
        } catch (e: Exception) {
            log.error("Error handling close for {}", connectionId, e)
        }
    }

    override suspend fun stop() {
        heartbeatManager.stopAll()
        protocol.router.stopServer()
    }
}