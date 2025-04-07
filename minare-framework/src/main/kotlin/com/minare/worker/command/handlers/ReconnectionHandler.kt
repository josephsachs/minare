package com.minare.worker.command.handlers

import com.google.inject.Inject
import com.minare.MinareApplication
import com.minare.cache.ConnectionCache
import com.minare.persistence.ConnectionStore
import com.minare.utils.ConnectionTracker
import com.minare.utils.HeartbeatManager
import com.minare.utils.VerticleLogger
import com.minare.worker.CleanupVerticle
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import com.minare.worker.command.ConnectionLifecycle
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
    /**
     * Handle a reconnection attempt
     */
    public suspend fun handle(websocket: ServerWebSocket, connectionId: String, deploymentId: String, traceId: String) {
        val reconnectTraceId = vlog.getEventLogger().trace(
            "RECONNECTION_ATTEMPT", mapOf(
                "connectionId" to connectionId,
                "socketId" to websocket.textHandlerID(),
                "remoteAddress" to websocket.remoteAddress().toString()
            )
        )

        try {
            vlog.logStartupStep(
                "HANDLING_RECONNECTION", mapOf(
                    "connectionId" to connectionId,
                    "traceId" to reconnectTraceId
                )
            )

            val exists = connectionStore.exists(connectionId)
            if (!exists) {
                vlog.getEventLogger().trace(
                    "RECONNECTION_FAILED", mapOf(
                        "reason" to "Connection not found",
                        "connectionId" to connectionId
                    ), reconnectTraceId
                )

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

            connectionCache.getCommandSocket(connectionId)?.let { existingSocket ->
                try {
                    if (!existingSocket.isClosed()) {
                        existingSocket.close()
                        vlog.getEventLogger().logWebSocketEvent(
                            "EXISTING_SOCKET_CLOSED", connectionId,
                            mapOf("socketId" to existingSocket.textHandlerID()), reconnectTraceId
                        )
                    }
                } catch (e: Exception) {
                    vlog.logVerticleError(
                        "CLOSE_EXISTING_SOCKET", e, mapOf(
                            "connectionId" to connectionId
                        )
                    )
                }
            }

            connectionCache.storeCommandSocket(connectionId, websocket, connection)
            connectionTracker.registerConnection(connectionId, reconnectTraceId, websocket)

            val socketId = "cs-${java.util.UUID.randomUUID()}"

            val updatedConnection = connectionStore.putUpdateSocket(
                connectionId,
                socketId,
                deploymentId
            )

            vertx.eventBus().publish(
                MinareApplication.ConnectionEvents.ADDRESS_COMMAND_SOCKET_CONNECTED,
                JsonObject()
                    .put("connectionId", connection._id)
                    .put("traceId", traceId)
            )

            vlog.getEventLogger().logStateChange(
                "Connection", "DISCONNECTED", "RECONNECTED",
                mapOf("connectionId" to connectionId, "socketId" to socketId), reconnectTraceId
            )

            sendReconnectionResponse(websocket, true, null)
            heartbeatManager.startHeartbeat(socketId, connectionId, websocket)

            if (connectionCache.isFullyConnected(connectionId)) {
                vlog.getEventLogger().logStateChange(
                    "Connection", "RECONNECTED", "FULLY_CONNECTED",
                    mapOf("connectionId" to connectionId), reconnectTraceId
                )

                // We want to know how to now call this on the ExampleApplication's connection controller
                //connectionController.onClientFullyConnected(updatedConnection)
            }

            vlog.getEventLogger().endTrace(
                reconnectTraceId, "RECONNECTION_COMPLETED",
                mapOf("connectionId" to connectionId, "success" to true)
            )

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