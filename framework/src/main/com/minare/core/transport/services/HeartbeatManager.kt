package com.minare.core.transport.services

import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.interfaces.SocketStore
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory

class HeartbeatManager(
    private val vertx: Vertx,
    private val connectionStore: ConnectionStore,
    private val coroutineScope: CoroutineScope,
    private val socketStore: SocketStore<*>
) {
    private val log = LoggerFactory.getLogger(HeartbeatManager::class.java)

    private val heartbeatTimers = HashMap<String, Long>()
    private var heartbeatIntervalMs = 15000L

    fun setHeartbeatInterval(intervalMs: Long) {
        heartbeatIntervalMs = intervalMs
    }

    fun startHeartbeat(socketId: String, connectionId: String) {
        stopHeartbeat(socketId)

        val timerId = vertx.setPeriodic(heartbeatIntervalMs) { _ ->
            coroutineScope.launch {
                try {
                    val sent = socketStore.send(connectionId, JsonObject()
                        .put("type", "heartbeat")
                        .put("timestamp", System.currentTimeMillis())
                        .encode()
                    )

                    if (!sent) {
                        stopHeartbeat(socketId)
                        return@launch
                    }

                    try {
                        connectionStore.updateLastActivity(connectionId)
                    } catch (e: Exception) {
                        log.warn("Failed to update last activity for connection {}", connectionId)
                    }
                } catch (e: Exception) {
                    log.warn("Heartbeat failed for socket {}: {}", socketId, e.message)
                    if (e.message?.contains("Connection was closed") == true) {
                        stopHeartbeat(socketId)
                    }
                }
            }
        }

        heartbeatTimers[socketId] = timerId
    }

    fun stopHeartbeat(socketId: String) {
        heartbeatTimers.remove(socketId)?.let { timerId ->
            vertx.cancelTimer(timerId)
        }
    }

    fun stopAll() {
        heartbeatTimers.forEach { (_, timerId) ->
            vertx.cancelTimer(timerId)
        }
        heartbeatTimers.clear()
    }

    fun activeCount(): Int = heartbeatTimers.size

    fun getMetrics(): JsonObject {
        return JsonObject()
            .put("active", heartbeatTimers.size)
            .put("intervalMs", heartbeatIntervalMs)
    }
}