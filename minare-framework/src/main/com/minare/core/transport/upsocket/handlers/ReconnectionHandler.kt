package com.minare.worker.upsocket.handlers

import com.google.inject.Inject
import com.google.inject.name.Named
import com.minare.core.MinareApplication
import com.minare.cache.ConnectionCache
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.downsocket.services.ConnectionTracker
import com.minare.utils.HeartbeatManager
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.transport.CleanupVerticle
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import com.minare.worker.upsocket.ConnectionLifecycle
import io.vertx.core.Vertx

class ReconnectionHandler @Inject constructor(
    private val vertx: Vertx,
    private val vlog: VerticleLogger,
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache,
    private val connectionTracker: ConnectionTracker,
    private val connectionLifecycle: ConnectionLifecycle,
    private val heartbeatManager: HeartbeatManager
) {
    private val debugTraceLogs: Boolean = false

    /**
     * Handle a reconnection attempt
     */
    suspend fun handle(websocket: ServerWebSocket, connectionId: String, deploymentId: String, traceId: String) {
        var reconnectTraceId: String = ""

        if (debugTraceLogs) {
            reconnectTraceId = vlog.getEventLogger().trace(
            "RECONNECTION_ATTEMPT", mapOf(
                "connectionId" to connectionId,
                "socketId" to websocket.textHandlerID(),
                "remoteAddress" to websocket.remoteAddress().toString())
            )
        }

        try {
            if (debugTraceLogs) {
                vlog.logStartupStep(
                    "HANDLING_RECONNECTION", mapOf(
                        "connectionId" to connectionId,
                        "traceId" to reconnectTraceId
                    )
                )
            }

            val exists = connectionStore.exists(connectionId)
            if (!exists) {
                if (debugTraceLogs) {
                    vlog.getEventLogger().trace(
                        "RECONNECTION_FAILED", mapOf(
                            "reason" to "Connection not found",
                            "connectionId" to connectionId
                        ), reconnectTraceId
                    )
                }

                sendReconnectionResponse(websocket, false, "Connection not found")
                connectionLifecycle.initiateConnection(websocket, deploymentId, traceId)  // Fallback to creating a new connection
                return
            }

            // We might be trying to connect the not-reconnectable
            val connection = connectionStore.find(connectionId)

            if (!connection.reconnectable) {
                vlog.getEventLogger().trace(
                    "RECONNECTION_FAILED", mapOf(
                        "reason" to "Connection not reconnectable",
                        "connectionId" to connectionId
                    ), reconnectTraceId
                )

                sendReconnectionResponse(websocket, false, "Connection not reconnectable")
                connectionLifecycle.initiateConnection(websocket, deploymentId, traceId)  // Fallback to creating a new connection

                return
            }

            val now = System.currentTimeMillis()
            val inactiveMs = now - connection.lastActivity
            val reconnectWindowMs = CleanupVerticle.CONNECTION_RECONNECT_WINDOW_MS

            if (inactiveMs > reconnectWindowMs) {
                vlog.getEventLogger().trace(
                    "RECONNECTION_FAILED", mapOf(
                        "reason" to "Reconnection window expired",
                        "connectionId" to connectionId,
                        "inactiveMs" to inactiveMs,
                        "windowMs" to reconnectWindowMs
                    ), reconnectTraceId
                )

                sendReconnectionResponse(websocket, false, "Reconnection window expired")
                connectionLifecycle.initiateConnection(websocket, deploymentId, traceId)  // Fallback to creating a new connection
                return
            }

            connectionCache.getUpSocket(connectionId)?.let { existingSocket ->
                try {
                    if (!existingSocket.isClosed()) {
                        existingSocket.close()
                        if (debugTraceLogs) {
                            vlog.getEventLogger().logWebSocketEvent(
                                "EXISTING_SOCKET_CLOSED", connectionId,
                                mapOf("socketId" to existingSocket.textHandlerID()), reconnectTraceId
                            )
                        }
                    }
                } catch (e: Exception) {
                    vlog.logVerticleError(
                        "CLOSE_EXISTING_SOCKET", e, mapOf(
                            "connectionId" to connectionId
                        )
                    )
                }
            }

            connectionCache.storeUpSocket(connectionId, websocket, connection)
            connectionTracker.registerConnection(connectionId, reconnectTraceId, websocket)

            val socketId = "cs-${java.util.UUID.randomUUID()}"

            val updatedConnection = connectionStore.putDownSocket(
                connectionId,
                socketId,
                deploymentId
            )

            vertx.eventBus().publish(
                MinareApplication.ConnectionEvents.ADDRESS_UP_SOCKET_CONNECTED,
                JsonObject()
                    .put("connectionId", connection._id)
                    .put("traceId", traceId)
            )

            if (debugTraceLogs) {
                vlog.getEventLogger().logStateChange(
                    "Connection", "DISCONNECTED", "RECONNECTED",
                    mapOf("connectionId" to connectionId, "socketId" to socketId), reconnectTraceId
                )
            }

            sendReconnectionResponse(websocket, true, null)
            heartbeatManager.startHeartbeat(socketId, connectionId, websocket)

            if (connectionCache.isFullyConnected(connectionId)) {
                if (debugTraceLogs) {
                    vlog.getEventLogger().logStateChange(
                        "Connection", "RECONNECTED", "FULLY_CONNECTED",
                        mapOf("connectionId" to connectionId), reconnectTraceId
                    )
                }

                // We want to know how to now call this on the ExampleApplication's connection controller
                //connectionController.onClientFullyConnected(updatedConnection)
            }

            if (debugTraceLogs) {
                vlog.getEventLogger().endTrace(
                    reconnectTraceId, "RECONNECTION_COMPLETED",
                    mapOf("connectionId" to connectionId, "success" to true)
                )
            }

        } catch (e: Exception) {
            vlog.logVerticleError(
                "RECONNECTION", e, mapOf(
                    "connectionId" to connectionId
                )
            )

            vlog.getEventLogger().logError(
                "RECONNECTION_ERROR", e,
                mapOf("connectionId" to connectionId), reconnectTraceId
            )

            sendReconnectionResponse(websocket, false, "Internal error")
            connectionLifecycle.initiateConnection(websocket, deploymentId, traceId)  // Fallback to creating a new connection
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
}