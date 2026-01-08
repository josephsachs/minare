package com.minare.worker.upsocket.handlers

import com.google.inject.Inject
import com.google.inject.name.Named
import com.minare.application.config.FrameworkConfig
import com.minare.cache.ConnectionCache
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.downsocket.services.ConnectionTracker
import com.minare.utils.HeartbeatManager
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.transport.CleanupVerticle
import io.vertx.core.http.ServerWebSocket

class CloseHandler @Inject constructor(
    private val vlog: VerticleLogger,
    private val frameworkConfig: FrameworkConfig,
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache,
    private val connectionTracker: ConnectionTracker,
    private val heartbeatManager: HeartbeatManager
) {
    private val debugTraceLogs: Boolean = false

    /**
     * Handle a socket being closed
     */
    public suspend fun handle(websocket: ServerWebSocket, connectionId: String) {
        val traceId = connectionTracker.getTraceId(connectionId)

        try {
            heartbeatManager.stopHeartbeat(connectionId)
            connectionStore.updateReconnectable(connectionId, true)

            // Remove socket from cache but don't delete connection yet
            connectionCache.removeUpSocket(connectionId)
            connectionTracker.handleSocketClosed(websocket)

            if (debugTraceLogs) {
                vlog.getEventLogger().logStateChange(
                    "Connection", "CONNECTED", "DISCONNECTED",
                    mapOf("connectionId" to connectionId), traceId
                )
            }

            if (debugTraceLogs) {
                vlog.getEventLogger().trace(
                    "RECONNECTION_WINDOW_STARTED", mapOf(
                        "connectionId" to connectionId,
                        "windowMs" to frameworkConfig.sockets.connection.reconnectTimeout
                    ), traceId
                )
            }
        } catch (e: Exception) {
            vlog.logVerticleError("WEBSOCKET_CLOSE", e, mapOf("connectionId" to connectionId))
        }
    }
}