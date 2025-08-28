package com.minare.worker.downsocket.events

import com.google.inject.Inject
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap
import com.minare.core.transport.downsocket.DownSocketVerticleCache

/**
 * Handles connection established event for downsocket
 */
class UpdateConnectionEstablishedEvent @Inject constructor(
    private val vlog: VerticleLogger,
    private val eventBusUtils: EventBusUtils,
    private val downSocketVerticleCache: DownSocketVerticleCache
) {
    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_CONNECTION_ESTABLISHED) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            if (connectionId != null) {
                downSocketVerticleCache.connectionPendingUpdates.computeIfAbsent(connectionId) { ConcurrentHashMap() }

                vlog.getEventLogger().trace("CONNECTION_TRACKING_INITIALIZED", mapOf(
                    "connectionId" to connectionId
                ), traceId)
            }
        }
        vlog.logHandlerRegistration(ADDRESS_CONNECTION_ESTABLISHED)
    }

    companion object {
        const val ADDRESS_CONNECTION_ESTABLISHED = "minare.connection.established"
    }
}