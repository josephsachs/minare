package com.minare.worker.update.events

import com.google.inject.Inject
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.update.UpdateVerticleCache

class UpdateConnectionClosedEvent @Inject constructor(
    private val vlog: VerticleLogger,
    private val eventBusUtils: EventBusUtils,
    private val updateVerticleCache: UpdateVerticleCache
) {
    suspend fun register() {
        // Register handler for connection closed events
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_CONNECTION_CLOSED) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            if (connectionId != null) {
                updateVerticleCache.connectionPendingUpdates.remove(connectionId)
                updateVerticleCache.invalidateChannelCacheForConnection(connectionId)

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