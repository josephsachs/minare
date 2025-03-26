package com.minare.worker.command.handlers

import com.google.inject.Inject
import com.minare.cache.ConnectionCache
import com.minare.persistence.ConnectionStore
import com.minare.utils.ConnectionTracker
import com.minare.utils.HeartbeatManager
import com.minare.utils.VerticleLogger
import com.minare.worker.CleanupVerticle
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import com.minare.worker.command.ConnectionLifecycle

class ReconnectionHandler @Inject constructor(
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
    public suspend fun handle(websocket: ServerWebSocket, connectionId: String, traceId: String) {
        // Create a trace for this reconnection
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
                connectionLifecycle.initiateConnection(websocket, traceId)  // Fallback to creating a new connection
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
                connectionLifecycle.initiateConnection(websocket, traceId)  // Fallback to creating a new connection

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
                connectionLifecycle.initiateConnection(websocket, traceId)  // Fallback to creating a new connection
                return
            }

            // Close existing command socket if any
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

            // Associate the new websocket with this connection
            connectionCache.storeCommandSocket(connectionId, websocket, connection)
            connectionTracker.registerConnection(connectionId, reconnectTraceId, websocket)

            // Create the actual update socket ID
            val socketId = "cs-${java.util.UUID.randomUUID()}"

            // We arrived, update the database with our new socket ID
            val updatedConnection = connectionStore.updateSocketIds(
                connectionId,
                socketId,
                connection.updateSocketId
            )

            vlog.getEventLogger().logStateChange(
                "Connection", "DISCONNECTED", "RECONNECTED",
                mapOf("connectionId" to connectionId, "socketId" to socketId), reconnectTraceId
            )

            sendReconnectionResponse(websocket, true, null)
            heartbeatManager.startHeartbeat(connectionId, websocket)

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
            connectionLifecycle.initiateConnection(websocket, traceId)  // Fallback to creating a new connection
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