package com.minare.worker.downsocket.events

import com.google.inject.Inject
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.core.transport.downsocket.DownSocketVerticleCache

/**
 * Handles connection closed event for downsocket
 */
class UpdateConnectionClosedEvent @Inject constructor(
    private val vlog: VerticleLogger,
    private val eventBusUtils: EventBusUtils,
    private val downSocketVerticleCache: DownSocketVerticleCache
) {
    suspend fun register(debugTraceLogs: Boolean) {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_CONNECTION_CLOSED) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            if (connectionId != null) {
                downSocketVerticleCache.connectionPendingUpdates.remove(connectionId)
                downSocketVerticleCache.invalidateChannelCacheForConnection(connectionId)

                if (debugTraceLogs) {
                    vlog.getEventLogger().trace("CONNECTION_TRACKING_REMOVED", mapOf(
                        "connectionId" to connectionId
                    ), traceId)
                }
            }
        }
        vlog.logHandlerRegistration(ADDRESS_CONNECTION_CLOSED)
    }

    companion object {
        const val ADDRESS_CONNECTION_CLOSED = "minare.connection.closed"
    }
}