package com.minare.worker.upsocket.events

import com.google.inject.Inject
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.upsocket.UpSocketVerticle
import com.minare.worker.upsocket.ConnectionLifecycle

class ConnectionCleanupEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val connectionLifecycle: ConnectionLifecycle
) {

    suspend fun register(debugTraceLogs: Boolean) {
        eventBusUtils.registerTracedConsumer<JsonObject>(UpSocketVerticle.ADDRESS_CONNECTION_CLEANUP) { message, traceId ->
            val connectionId = message.body().getString("connectionId")

            if (debugTraceLogs) {
                vlog.logStartupStep("CONNECTION_CLEANUP_REQUEST", mapOf("connectionId" to connectionId))
            }

            try {
                val result = connectionLifecycle.cleanupConnection(connectionId)
                eventBusUtils.tracedReply(message, JsonObject().put("success", result), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError("CONNECTION_CLEANUP", e, mapOf("connectionId" to connectionId))
                message.fail(500, e.message ?: "Error during connection cleanup")
            }
        }

    }
}