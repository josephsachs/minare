package com.minare.core.transport.downsocket

import com.google.inject.Inject
import com.minare.application.config.FrameworkConfig
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ContextStore
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap

class DownSocketVerticleCache @Inject constructor(
    private val frameworkConfig: FrameworkConfig,
    private val contextStore: ContextStore
) {
    val connectionPendingUpdates = ConcurrentHashMap<String, MutableMap<String, JsonObject>>()

    /**
     * Queue an entity update for a specific connection.
     * If the entity already has a pending update, it will be replaced with the newer version.
     */
    fun queueUpdateForConnection(connectionId: String, entityId: String, entityUpdate: JsonObject) {
        val updates = connectionPendingUpdates.computeIfAbsent(connectionId) { ConcurrentHashMap() }

        // Check if we already have an update for this entity
        val existingUpdate = updates[entityId]
        if (existingUpdate != null) {
            // Compare versions and only replace if newer
            val existingVersion = existingUpdate.getLong("version", 0)
            val newVersion = entityUpdate.getLong("version", 0)

            if (newVersion > existingVersion) {
                // New version is higher, replace the pending update
                updates[entityId] = entityUpdate
            }
        } else {
            // No existing update, add this one
            updates[entityId] = entityUpdate
        }
    }
}