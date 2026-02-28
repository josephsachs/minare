package com.minare.core.transport

import com.google.inject.Inject
import com.minare.application.config.FrameworkConfig
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.upsocket.UpSocketVerticle
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory

class CleanupVerticle @Inject constructor(
    private val frameworkConfig: FrameworkConfig,
    private val connectionStore: ConnectionStore,
    private val debug: DebugLogger
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(CleanupVerticle::class.java)

    companion object {
        const val ADDRESS_TRIGGER_CLEANUP = "minare.trigger.cleanup"
    }

    private val reconnectTimeout = frameworkConfig.sockets.connection.reconnectTimeout
    private val cleanupInterval = frameworkConfig.sockets.connection.cleanupInterval
    private val connectionExpiry = frameworkConfig.sockets.connection.connectionExpiry
    private val aggressiveCleanup = frameworkConfig.sockets.connection.aggressiveCleanup

    override suspend fun start() {
        vertx.setPeriodic(cleanupInterval) { _ ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val removed = performCleanup(aggressiveCleanup)
                    debug.log(DebugType.CLEANUP_VERTICLE_METRICS, listOf(removed, 0))
                } catch (e: Exception) {
                    log.error("Error during scheduled cleanup", e)
                }
            }
        }

        vertx.eventBus().consumer<JsonObject>(ADDRESS_TRIGGER_CLEANUP) { message ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val forceAggressive = message.body().getBoolean("forceAggressive", false)
                    val removed = performCleanup(forceAggressive)
                    message.reply(JsonObject()
                        .put("success", true)
                        .put("connectionsRemoved", removed)
                    )
                } catch (e: Exception) {
                    log.error("Error during triggered cleanup", e)
                    message.reply(JsonObject().put("success", false).put("error", e.message))
                }
            }
        }

        log.info("CleanupVerticle started")
    }

    private suspend fun performCleanup(aggressive: Boolean = false): Int {
        debug.log(DebugType.CLEANUP_STARTING_PROCESS, listOf(aggressive))

        var removed = 0
        val connectionsToClear = mutableListOf<String>()

        val expired = connectionStore.findInactiveConnections(connectionExpiry)
        debug.log(DebugType.CLEANUP_FOUND_INACTIVE_CONNECTIONS, listOf(expired.size, connectionExpiry / 1000 / 60))
        connectionsToClear.addAll(expired.map { it.id })

        if (aggressive) {
            val disconnected = connectionStore.findInactiveConnections(reconnectTimeout)
                .filter { !it.reconnectable || it.upSocketId == null }
            debug.log(DebugType.CLEANUP_FOUND_NON_RECONNECTABLE, listOf(disconnected.size))
            connectionsToClear.addAll(disconnected.map { it.id })
        }

        for (connectionId in connectionsToClear.distinct()) {
            if (cleanupConnection(connectionId)) removed++
        }

        return removed
    }

    private suspend fun cleanupConnection(connectionId: String): Boolean {
        return try {
            vertx.eventBus().request<JsonObject>(
                UpSocketVerticle.ADDRESS_CONNECTION_CLEANUP,
                JsonObject().put("connectionId", connectionId)
            ).await()

            connectionStore.delete(connectionId)
            true
        } catch (e: Exception) {
            log.error("Failed to clean up connection {}", connectionId, e)
            try {
                connectionStore.delete(connectionId)
                true
            } catch (deleteEx: Exception) {
                log.error("Failed to delete connection {} from store", connectionId, deleteEx)
                false
            }
        }
    }
}