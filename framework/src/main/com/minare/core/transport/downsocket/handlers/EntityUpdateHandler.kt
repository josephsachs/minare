package com.minare.core.transport.downsocket.handlers

import com.google.inject.Inject
import com.minare.core.storage.adapters.HazelcastContextStore
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.downsocket.DownSocketVerticle
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject

/**
 * Centralizes behavior for entity update message handling.
 * Routes each update to the specific DownSocketVerticle instance that owns the
 * target connection, using the connection's stored downSocketDeploymentId.
 */
class EntityUpdateHandler @Inject constructor(
    private val vertx: Vertx,
    private val vlog: VerticleLogger,
    private val connectionStore: ConnectionStore,
    private val contextStore: HazelcastContextStore,
    private val channelStore: ChannelStore
) {
    /**
     * Handle an entity update from the change stream.
     * Looks up all connections subscribed to the entity's channels and sends
     * a targeted message to the DownSocketVerticle instance that owns each connection.
     */
    suspend fun handle(entityUpdate: JsonObject, traceId: String) {
        try {
            val entityId = entityUpdate.getString("_id")

            if (entityId == null) {
                vlog.getEventLogger().trace("ENTITY_UPDATE_MISSING_ID", mapOf(
                    "update" to entityUpdate.encode()
                ), traceId)
                return
            }

            val startTime = System.currentTimeMillis()
            val channels = contextStore.getChannelsByEntityId(entityId)
            val lookupTime = System.currentTimeMillis() - startTime

            if (lookupTime > 50) {
                vlog.getEventLogger().logPerformance("CHANNEL_LOOKUP", lookupTime, mapOf(
                    "entityId" to entityId,
                    "channelCount" to channels.size
                ), traceId)
            }

            if (channels.isEmpty()) {
                vlog.getEventLogger().trace("ENTITY_NO_CHANNELS", mapOf(
                    "entityId" to entityId
                ), traceId)
                return
            }

            val processedConnections = mutableSetOf<String>()

            for (channelId in channels) {
                val connections = connectionStore.find(
                    channelStore.getClientIds(channelId).toSet()
                )

                for (connection in connections) {
                    if (connection.id in processedConnections) continue
                    processedConnections.add(connection.id)

                    val downInstanceId = connection.downSocketDeploymentId
                    if (downInstanceId == null) {
                        vlog.getEventLogger().trace("UPDATE_SKIPPED_NO_DOWN_INSTANCE", mapOf(
                            "entityId" to entityId,
                            "connectionId" to connection.id
                        ), traceId)
                        continue
                    }

                    vertx.eventBus().send(
                        "${DownSocketVerticle.ADDRESS_SEND_TO_DOWN_CONNECTION}.${downInstanceId}",
                        JsonObject()
                            .put("connectionId", connection.id)
                            .put("entityId", entityId)
                            .put("update", entityUpdate)
                    )
                }
            }

            if (processedConnections.isNotEmpty()) {
                vlog.getEventLogger().trace("UPDATE_ROUTED", mapOf(
                    "entityId" to entityId,
                    "connectionCount" to processedConnections.size
                ), traceId)
            }
        } catch (e: Exception) {
            vlog.logVerticleError("ENTITY_UPDATE_PROCESSING", e, mapOf(
                "traceId" to traceId
            ))
        }
    }
}