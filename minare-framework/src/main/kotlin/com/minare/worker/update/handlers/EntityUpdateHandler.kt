package com.minare.worker.update.handlers

import com.google.inject.Inject
import com.minare.persistence.ConnectionStore
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.update.UpdateVerticleCache

class EntityUpdateHandler @Inject constructor(
    private val vlog: VerticleLogger,
    private val updateVerticleCache: UpdateVerticleCache,
    private val connectionStore: ConnectionStore
) {
    private var deploymentId: String? = null

    /**
     * Set the deployment ID for this handler instance.
     * This should be called during event registration to establish which
     * UpdateVerticle instance this handler belongs to.
     */
    fun setDeploymentId(deploymentId: String) {
        this.deploymentId = deploymentId
        vlog.getEventLogger().trace("HANDLER_DEPLOYMENT_ID_SET", mapOf(
            "deploymentId" to deploymentId
        ))
    }

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

            /**val currentDeploymentId = this.deploymentId
                ?: error("Deployment ID not set on EntityUpdateHandler - register() was not called properly")**/

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
            var hasOwnedConnections = false

            for (channelId in channels) {
                val connections = connectionStore.find(
                    updateVerticleCache.getConnectionsForChannel(channelId)
                )

                for (connection in connections) {
                    if (connection._id in processedConnections) {
                        continue
                    }

                    /**if (connection.updateDeploymentId != currentDeploymentId) {
                        vlog.getEventLogger().trace("CONNECTION_DEPLOYMENT_CHECK", mapOf(
                            "connectionId" to connection._id,
                            "connectionUpdateDeploymentId" to connection.updateDeploymentId,
                            "currentDeploymentId" to currentDeploymentId,
                            "matches" to "false"
                        ), traceId)
                        continue
                    } else {
                        vlog.getEventLogger().trace("CONNECTION_DEPLOYMENT_CHECK", mapOf(
                            "connectionId" to connection._id,
                            "connectionUpdateDeploymentId" to connection.updateDeploymentId,
                            "currentDeploymentId" to currentDeploymentId,
                            "matches" to "true"
                        ), traceId)
                    }**/

                    hasOwnedConnections = true
                    processedConnections.add(connection._id)

                    // Queue update for this connection
                    updateVerticleCache.queueUpdateForConnection(connection._id, entityId, entityUpdate)
                }
            }

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