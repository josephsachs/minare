package com.minare.worker.downsocket.events

import com.google.inject.Inject
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.downsocket.DownSocketVerticleCache

class UpdateConnectionClosedEvent @Inject constructor(
    private val vlog: VerticleLogger,
    private val eventBusUtils: EventBusUtils,
    private val downSocketVerticleCache: DownSocketVerticleCache
) {
    suspend fun register() {
        // Register handler for connection closed events
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_CONNECTION_CLOSED) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            if (connectionId != null) {
                downSocketVerticleCache.connectionPendingUpdates.remove(connectionId)
                downSocketVerticleCache.invalidateChannelCacheForConnection(connectionId)

                vlog.getEventLogger().trace("CONNECTION_TRACKING_REMOVED", mapOf(
                    "connectionId" to connectionId
                ), traceId)
            }
        }
        vlog.logHandlerRegistration(ADDRESS_CONNECTION_CLOSED)
    }

    companion object {
        const val ADDRESS_CONNECTION_CLOSED = "minare.connection.closed"
    }
}