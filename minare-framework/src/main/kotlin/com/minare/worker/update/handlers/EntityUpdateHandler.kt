package com.minare.worker.update.handlers

import com.google.inject.Inject
import com.minare.persistence.ConnectionStore
import com.minare.persistence.MongoConnectionStore
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.update.UpdateVerticleCache
import io.vertx.core.Vertx

class EntityUpdateHandler @Inject constructor(
    private val vlog: VerticleLogger,
    private val updateVerticleCache: UpdateVerticleCache,
    private val connectionStore: ConnectionStore
) {
    /**
     * Handle an entity update from the change stream.
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

            val currentDeploymentId = Vertx.currentContext().deploymentID()
            val startTime = System.currentTimeMillis()
            val channels = updateVerticleCache.getChannelsForEntity(entityId)
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
            var hasOwnedConnections = false  // Add this flag

            for (channelId in channels) {
                val connections = connectionStore.find(
                        updateVerticleCache.getConnectionsForChannel(channelId)
                )

                for (connection in connections) {
                    if (connection._id in processedConnections) {
                        continue
                    }

                    if (connection.updateDeploymentId != currentDeploymentId) {
                        continue
                    }

                    hasOwnedConnections = true
                    processedConnections.add(connection._id)

                    // Queue update for this connection
                    updateVerticleCache.queueUpdateForConnection(connection._id, entityId, entityUpdate)
                }
            }

            // Add early return check if we didn't process any connections
            if (!hasOwnedConnections) {
                vlog.getEventLogger().trace("UPDATE_SKIPPED_NO_OWNED_CONNECTIONS", mapOf(
                    "entityId" to entityId
                ), traceId)
                return
            }

            if (processedConnections.isNotEmpty()) {
                vlog.getEventLogger().trace("UPDATE_QUEUED", mapOf(
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