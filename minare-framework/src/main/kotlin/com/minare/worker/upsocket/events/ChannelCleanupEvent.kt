package com.minare.worker.upsocket.events

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.upsocket.ConnectionLifecycle

@Singleton
class ChannelCleanupEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val connectionLifecycle: ConnectionLifecycle
) {
    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_CHANNEL_CLEANUP) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            vlog.logStartupStep("CHANNEL_CLEANUP_REQUEST", mapOf("connectionId" to connectionId))

            try {
                val result = connectionLifecycle.cleanupConnectionChannels(connectionId)
                eventBusUtils.tracedReply(message, JsonObject().put("success", result), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError("CHANNEL_CLEANUP", e, mapOf("connectionId" to connectionId))
                message.fail(500, e.message ?: "Error during channel cleanup")
            }
        }
    }

    companion object {
        const val ADDRESS_CHANNEL_CLEANUP = "minare.channel.cleanup"
    }
}