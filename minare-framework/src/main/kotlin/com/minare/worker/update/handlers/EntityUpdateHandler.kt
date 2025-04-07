package com.minare.worker.update.handlers

import com.google.inject.Inject
import com.minare.utils.ConnectionTracker
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.update.UpdateVerticleCache
import io.vertx.core.Vertx

class EntityUpdateHandler @Inject constructor(
    private val vlog: VerticleLogger,
    private val updateVerticleCache: UpdateVerticleCache
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

            // Get channels for this entity (from cache or database)
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
                // No channels, no need to process further
                vlog.getEventLogger().trace("ENTITY_NO_CHANNELS", mapOf(
                    "entityId" to entityId
                ), traceId)
                return
            }

            // For each channel, get all connections and queue the update
            val processedConnections = mutableSetOf<String>()
            var hasOwnedConnections = false  // Add this flag

            for (channelId in channels) {
                val connections = updateVerticleCache.getConnectionsForChannel(channelId)

                for (connectionId in connections) {
                    // Avoid processing the same connection multiple times
                    if (connectionId in processedConnections) {
                        continue
                    }

                    // Skip if this verticle doesn't own the connection
                    if (!Vertx.currentContext().isLocal(connectionId)) {
                        continue
                    }

                    hasOwnedConnections = true  // Set flag when we find an owned connection
                    processedConnections.add(connectionId)

                    // Queue update for this connection
                    updateVerticleCache.queueUpdateForConnection(connectionId, entityId, entityUpdate)
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