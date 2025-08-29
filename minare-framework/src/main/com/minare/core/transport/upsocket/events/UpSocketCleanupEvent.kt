package com.minare.worker.upsocket.events

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.upsocket.UpSocketVerticle
import com.minare.worker.upsocket.ConnectionLifecycle

/**
 * Note that a defunct upsocket should not try to reconnect automatically, this should be
 * kicked back to client reconnect handling
 */
@Singleton
class UpSocketCleanupEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val connectionLifecycle: ConnectionLifecycle
) {
    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(UpSocketVerticle.ADDRESS_SOCKET_CLEANUP) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            val hasUpdateSocket = message.body().getBoolean("hasUpdateSocket", false)
            vlog.logStartupStep(
                "SOCKET_CLEANUP_REQUEST", mapOf(
                    "connectionId" to connectionId,
                    "hasUpdateSocket" to hasUpdateSocket
                )
            )

            try {
                val result = connectionLifecycle.cleanupConnectionSockets(connectionId, hasUpdateSocket)
                eventBusUtils.tracedReply(message, JsonObject().put("success", result), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError("SOCKET_CLEANUP", e, mapOf("connectionId" to connectionId))
                message.fail(500, e.message ?: "Error during socket cleanup")
            }
        }
    }
}