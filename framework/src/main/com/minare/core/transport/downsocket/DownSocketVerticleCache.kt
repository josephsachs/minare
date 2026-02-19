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
    private val channelStore: ChannelStore,
    private val contextStore: ContextStore,
    private val vlog: VerticleLogger
) {
    val entityChannelCache = ConcurrentHashMap<String, Pair<Set<String>, Long>>()
    val channelConnectionCache = ConcurrentHashMap<String, Pair<Set<String>, Long>>()
    val connectionPendingUpdates = ConcurrentHashMap<String, MutableMap<String, JsonObject>>()

    /**
     * Invalidate any channel cache entries containing this connection
     */
    fun invalidateChannelCacheForConnection(connectionId: String) {
        for ((channelId, entry) in channelConnectionCache) {
            val (connections, _) = entry
            if (connectionId in connections) {
                channelConnectionCache.remove(channelId)
            }
        }
    }

    private suspend fun tryOrInvalidate(key: String, cache: ConcurrentHashMap<String, Pair<Set<String>, Long>>): Set<String> {
        val now = System.currentTimeMillis()
        val entry = cache[key]
        if (entry != null) {
            val (items, expiry) = entry
            if (now < expiry) {
                // Cache entry is still valid
                return items
            }
            // Cache entry expired, remove it
            cache.remove(key)
        }
        return emptySet()
    }

    /**
     * Get all channels that contain a specific entity.
     * Uses a cache with TTL to reduce database queries.
     */
    suspend fun getChannelsForEntity(entityId: String): Set<String> {
        return tryOrInvalidate(entityId, entityChannelCache).ifEmpty {
            contextStore.getChannelsByEntityId(entityId).toSet().also {
                entityChannelCache[entityId] = Pair(it, System.currentTimeMillis() + frameworkConfig.sockets.down.cacheTtl)
            }
        }
    }

    /**
     * Get all connections subscribed to a specific channel.
     * Uses a cache with TTL to reduce database queries.
     */
    suspend fun getConnectionsForChannel(channelId: String): Set<String> {
        return tryOrInvalidate(channelId, channelConnectionCache).ifEmpty {
            contextStore.getChannelsByEntityId(channelId).toSet().also {
                entityChannelCache[channelId] = Pair(it, System.currentTimeMillis() + frameworkConfig.sockets.down.cacheTtl)
            }
        }
    }

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