package com.minare.utils

import com.minare.persistence.ConnectionStore
import io.vertx.core.Vertx
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap

/**
 * Manages heartbeats for WebSocket connections to detect disconnections.
 * Provides a central place to handle heartbeat logic shared by verticles.
 */
class HeartbeatManager(
    private val vertx: Vertx,
    private val logger: VerticleLogger,
    private val connectionStore: ConnectionStore,
    private val coroutineScope: CoroutineScope
) {
    private val log = LoggerFactory.getLogger(HeartbeatManager::class.java)

    private val heartbeatTimers = ConcurrentHashMap<String, Long>()
    private var heartbeatIntervalMs = 15000L

    /**
     * Set the heartbeat interval
     *
     * @param intervalMs Interval in milliseconds
     */
    fun setHeartbeatInterval(intervalMs: Long) {
        heartbeatIntervalMs = intervalMs
        log.info("Heartbeat interval set to {}ms", intervalMs)
    }

    /**
     * Start heartbeat for a socket
     *
     * @param socketId WebSocket ID
     * @param connectionId Associated connection ID (for activity updates)
     * @param socket WebSocket to send heartbeats on
     */
    fun startHeartbeat(socketId: String, connectionId: String, socket: ServerWebSocket) {
        stopHeartbeat(socketId)

        val timerId = vertx.setPeriodic(heartbeatIntervalMs) { _ ->
            coroutineScope.launch {
                try {
                    if (socket.isClosed()) {
                        stopHeartbeat(socketId)
                        return@launch
                    }

                    val heartbeatMessage = JsonObject()
                        .put("type", "heartbeat")
                        .put("timestamp", System.currentTimeMillis())

                    socket.writeTextMessage(heartbeatMessage.encode())

                    try {
                        connectionStore.updateLastActivity(connectionId)
                    } catch (e: Exception) {
                        log.warn("Failed to update last activity for connection $connectionId", e)
                    }

                    if (Math.random() < 0.05) { // % of heartbeats
                        logger.getEventLogger().trace("HEARTBEAT_SENT", mapOf(
                            "socketId" to socketId,
                            "connectionId" to connectionId
                        ))
                    }
                } catch (e: Exception) {
                    logger.logVerticleError("HEARTBEAT_SEND", e, mapOf(
                        "socketId" to socketId,
                        "connectionId" to connectionId
                    ))

                    if (e.message?.contains("Connection was closed") == true) {
                        stopHeartbeat(socketId)
                    }
                }
            }
        }


        heartbeatTimers[socketId] = timerId
        logger.getEventLogger().trace("HEARTBEAT_STARTED", mapOf(
            "socketId" to socketId,
            "connectionId" to connectionId,
            "intervalMs" to heartbeatIntervalMs
        ))
    }

    /**
     * Stop heartbeat for a socket
     *
     * @param socketId Socket ID
     */
    fun stopHeartbeat(socketId: String) {
        heartbeatTimers.remove(socketId)?.let { timerId ->
            vertx.cancelTimer(timerId)
            logger.getEventLogger().trace("HEARTBEAT_STOPPED", mapOf(
                "socketId" to socketId
            ))
        }
    }

    /**
     * Handle a heartbeat response from a client
     *
     * @param connectionId Connection ID
     * @param message Heartbeat response message
     */
    fun handleHeartbeatResponse(connectionId: String, message: JsonObject) {
        try {

            val serverTimestamp = message.getLong("timestamp")
            val clientTimestamp = message.getLong("clientTimestamp", 0L)
            val now = System.currentTimeMillis()
            val roundTripTime = now - serverTimestamp

            if (Math.random() < 0.1) { // % of heartbeat responses
                logger.getEventLogger().trace("HEARTBEAT_RESPONSE", mapOf(
                    "connectionId" to connectionId,
                    "roundTripMs" to roundTripTime,
                    "clientTimestamp" to clientTimestamp
                ))
            }
        } catch (e: Exception) {
            logger.logVerticleError("HEARTBEAT_PROCESSING", e, mapOf(
                "connectionId" to connectionId
            ))
        }
    }

    /**
     * Stop all heartbeats
     */
    fun stopAll() {
        heartbeatTimers.forEach { (socketId, timerId) ->
            vertx.cancelTimer(timerId)
            log.debug("Stopped heartbeat for socket $socketId")
        }
        heartbeatTimers.clear()
        log.info("Stopped all heartbeats")
    }

    /**
     * Get the number of active heartbeat timers
     *
     * @return Count of active heartbeats
     */
    fun getActiveHeartbeatCount(): Int {
        return heartbeatTimers.size
    }

    /**
     * Get metrics about heartbeats
     *
     * @return JsonObject with heartbeat metrics
     */
    fun getMetrics(): JsonObject {
        return JsonObject()
            .put("heartbeats", JsonObject()
                .put("active", heartbeatTimers.size)
                .put("intervalMs", heartbeatIntervalMs)
            )
    }
}