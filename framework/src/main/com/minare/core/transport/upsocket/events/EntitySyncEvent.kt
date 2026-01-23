package com.minare.worker.upsocket.events

import com.google.inject.Inject
import com.minare.cache.ConnectionCache
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.storage.interfaces.EntityGraphStore
import com.minare.core.transport.downsocket.services.ConnectionTracker
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.upsocket.UpSocketVerticle

/**
 * Handles Entity sync requests
 *
 * TODO: This should return results via downsocket to preserve unidirectional flow
 */
class EntitySyncEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val connectionCache: ConnectionCache,
    private val entityGraphStore: EntityGraphStore,
    private val connectionStore: ConnectionStore
) {
    private val connectionTracker = ConnectionTracker("UpSocket", vlog)
    private var debugTraceLogs: Boolean = false

    suspend fun register(debugTraceLogs: Boolean) {
        // Quiet, you
        this.debugTraceLogs = debugTraceLogs

        eventBusUtils.registerTracedConsumer<JsonObject>(UpSocketVerticle.ADDRESS_ENTITY_SYNC) { message, traceId ->
            val connectionId = message.body().getString("connectionId")
            val entityId = message.body().getString("entityId")

            try {
                if (debugTraceLogs) {
                    vlog.logStartupStep(
                        "ENTITY_SYNC_REQUEST", mapOf(
                            "entityId" to entityId,
                            "connectionId" to connectionId,
                            "traceId" to traceId
                        )
                    )
                }

                val result = handleEntitySync(connectionId, entityId)

                eventBusUtils.tracedReply(message, JsonObject().put("success", result), traceId)
            } catch (e: Exception) {
                vlog.logVerticleError(
                    "ENTITY_SYNC", e, mapOf(
                        "entityId" to entityId,
                        "connectionId" to connectionId
                    )
                )
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
            if (debugTraceLogs) {
                vlog.getEventLogger().trace(
                    "ENTITY_SYNC_STARTED", mapOf(
                        "entityId" to entityId,
                        "connectionId" to connectionId
                    ), traceId
                )
            }

            // Check if connection and command socket exist
            if (!connectionCache.hasConnection(connectionId)) {
                vlog.getEventLogger().trace(
                    "CONNECTION_NOT_FOUND", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
                return false
            }

            val upSocket = connectionCache.getUpSocket(connectionId)
            if (upSocket == null || upSocket.isClosed()) {
                vlog.getEventLogger().trace(
                    "COMMAND_SOCKET_UNAVAILABLE", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
                return false
            }

            // Fetch the entity
            val startTime = System.currentTimeMillis()

            if (debugTraceLogs) {
                vlog.getEventLogger().logDbOperation(
                    "FIND", "entity_graph",
                    mapOf("entityId" to entityId), traceId
                )
            }

            val entities = entityGraphStore.findEntitiesByIds(listOf(entityId))

            val queryTime = System.currentTimeMillis() - startTime

            if (debugTraceLogs) {
                vlog.getEventLogger().logPerformance(
                    "ENTITY_QUERY", queryTime,
                    mapOf("entityId" to entityId), traceId
                )
            }

            if (entities.isEmpty()) {
                if (debugTraceLogs) {
                    vlog.getEventLogger().trace(
                        "ENTITY_NOT_FOUND", mapOf(
                            "entityId" to entityId
                        ), traceId
                    )
                }
                sendSyncErrorToClient(connectionId, "Entity not found")
                return false
            }

            val entity = entities[entityId]

            // Create a sync response message
            val syncData = JsonObject()
                .put(
                    "entity_graph", JsonObject()
                        .put("_id", entity?._id)
                        .put("type", entity?.type)
                        .put("version", entity?.version)
                    // Add more entity fields as needed
                )
                .put("timestamp", System.currentTimeMillis())

            val syncMessage = JsonObject()
                .put("type", "entity_sync")
                .put("data", syncData)

            // Send the sync message to the client
            upSocket.writeTextMessage(syncMessage.encode())

            if (debugTraceLogs) {
                vlog.getEventLogger().trace(
                    "ENTITY_SYNC_DATA_SENT", mapOf(
                        "entityId" to entityId,
                        "connectionId" to connectionId
                    ), traceId
                )
            }

            // Update last activity
            connectionStore.updateLastActivity(connectionId)

            if (debugTraceLogs) {
                vlog.getEventLogger().trace(
                    "ENTITY_SYNC_COMPLETED", mapOf(
                        "entityId" to entityId,
                        "connectionId" to connectionId
                    ), traceId
                )
            }

            return true
        } catch (e: Exception) {
            vlog.logVerticleError(
                "ENTITY_SYNC", e, mapOf(
                    "entityId" to entityId,
                    "connectionId" to connectionId
                )
            )
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

                if (debugTraceLogs) {
                    vlog.getEventLogger().trace(
                        "SYNC_ERROR_SENT", mapOf(
                            "connectionId" to connectionId,
                            "error" to errorMessage
                        )
                    )
                }
            } catch (e: Exception) {
                vlog.logVerticleError(
                    "SYNC_ERROR_SEND", e, mapOf(
                        "connectionId" to connectionId
                    )
                )
            }
        }
    }
}