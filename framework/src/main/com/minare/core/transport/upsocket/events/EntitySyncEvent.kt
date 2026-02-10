package com.minare.core.transport.upsocket.events

import com.google.inject.Inject
import com.minare.cache.ConnectionCache
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.transport.downsocket.services.ConnectionTracker
import com.minare.core.transport.upsocket.UpSocketVerticle
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType

/**
 * Handles Entity sync requests
 *
 * This has largely been replaced by SyncCommandHandler but is maintained for cases in which
 * the application wants to trigger resync using an event rather than the message command interface.
 */
class EntitySyncEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val connectionCache: ConnectionCache,
    private val connectionStore: ConnectionStore,
    private val stateStore: StateStore,
    private val debug: DebugLogger
) {
    private val connectionTracker = ConnectionTracker("UpSocket", vlog)

    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(UpSocketVerticle.ADDRESS_ENTITY_SYNC) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            val entityId = message.body().getString("entityId")

            try {
                val result = handleEntitySync(connectionId, entityId)

                eventBusUtils.tracedReply(message, JsonObject().put("success", result), traceId)
            } catch (e: Exception) {
                debug.log(DebugType.UPSOCKET_ENTITY_SYNC_EVENT_ERROR, listOf(connectionId))

                message.fail(500, e.message ?: "Error handling entity sync")
            }
        }
    }

    /**
     * Handle entity-specific sync request
     */
    private suspend fun handleEntitySync(connectionId: String, entityId: String): Boolean {
        val traceId = connectionTracker.getTraceId(connectionId)

        try {
            if (!connectionCache.hasConnection(connectionId)) {
                vlog.getEventLogger().trace("CONNECTION_NOT_FOUND", mapOf("connectionId" to connectionId), traceId)
                return false
            }

            val upSocket = connectionCache.getUpSocket(connectionId)

            if (upSocket == null || upSocket.isClosed) {
                vlog.getEventLogger().trace("COMMAND_SOCKET_UNAVAILABLE", mapOf("connectionId" to connectionId), traceId)
                return false
            }

            val entities = stateStore.findJsonByIds(listOf(entityId))

            if (entities.isEmpty()) {
                vlog.getEventLogger().trace("ENTITY_NOT_FOUND", mapOf("entityId" to entityId), traceId)
                sendSyncErrorToClient(connectionId, "Entity not found")
                return false
            }

            val entity = entities.getOrElse(entityId, { return false })

            val syncMessage = JsonObject()
                .put("type", "entity_sync")
                .put("data", entity
                    .put("timestamp", System.currentTimeMillis()))

            upSocket.writeTextMessage(syncMessage.encode())

            connectionStore.updateLastActivity(connectionId)

            return true
        } catch (e: Exception) {
            vlog.logVerticleError("ENTITY_SYNC", e, mapOf("entityId" to entityId, "connectionId" to connectionId))
            sendSyncErrorToClient(connectionId, "Sync failed: ${e.message}")
            return false
        }
    }

    /**
     * Send a sync error message to the client
     */
    private fun sendSyncErrorToClient(connectionId: String, errorMessage: String) {
        val socket = connectionCache.getUpSocket(connectionId)
        if (socket != null && !socket.isClosed()) {
            try {
                val errorResponse = JsonObject()
                    .put("type", "sync_error")
                    .put("error", errorMessage)
                    .put("timestamp", System.currentTimeMillis())

                socket.writeTextMessage(errorResponse.encode())
            } catch (e: Exception) {
                vlog.logVerticleError("SYNC_ERROR_SEND", e, mapOf("connectionId" to connectionId))
            }
        }
    }
}