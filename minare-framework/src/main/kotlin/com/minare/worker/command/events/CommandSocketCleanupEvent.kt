package com.minare.worker.command.events

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.command.CommandVerticle
import com.minare.worker.command.CommandVerticle.Companion.ADDRESS_SOCKET_CLEANUP
import com.minare.worker.command.ConnectionLifecycle

@Singleton
class CommandSocketCleanupEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val connectionLifecycle: ConnectionLifecycle
) {
    suspend fun register(context: CommandVerticle) {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_SOCKET_CLEANUP) { message, traceId ->
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