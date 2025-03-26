package com.minare.worker.command.events

import com.google.inject.Inject
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.command.CommandVerticle
import com.minare.worker.command.CommandVerticle.Companion.ADDRESS_CONNECTION_CLEANUP
import com.minare.worker.command.ConnectionLifecycle

class ConnectionCleanupEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val connectionLifecycle: ConnectionLifecycle
) {

    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_CONNECTION_CLEANUP) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            vlog.logStartupStep("CONNECTION_CLEANUP_REQUEST", mapOf("connectionId" to connectionId))

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