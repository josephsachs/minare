package com.minare.worker.update

import com.google.inject.Inject
import com.minare.persistence.ChannelStore
import com.minare.persistence.ContextStore
import com.minare.utils.VerticleLogger
import com.minare.worker.update.UpdateVerticle.Companion.CACHE_TTL_MS
import io.vertx.core.json.JsonObject
import java.util.concurrent.ConcurrentHashMap

class UpdateVerticleCache @Inject constructor(
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
    suspend fun invalidateChannelCacheForConnection(connectionId: String) {
        for ((channelId, entry) in channelConnectionCache) {
            val (connections, _) = entry
            if (connectionId in connections) {
                channelConnectionCache.remove(channelId)
            }
        }
    }

    /**
     * Get all channels that contain a specific entity.
     * Uses a cache with TTL to reduce database queries.
     */
    suspend fun getChannelsForEntity(entityId: String): Set<String> {
        val now = System.currentTimeMillis()

        // Check cache first
        val cachedEntry = entityChannelCache[entityId]
        if (cachedEntry != null) {
            val (channels, expiry) = cachedEntry
            if (now < expiry) {
                // Cache entry is still valid
                return channels
            }
            // Cache entry expired, remove it
            entityChannelCache.remove(entityId)
        }

        // Query database
        val channels = try {
            contextStore.getChannelsByEntityId(entityId).toSet()
        } catch (e: Exception) {
            vlog.logVerticleError("CHANNEL_LOOKUP", e, mapOf(
                "entityId" to entityId
            ))
            emptySet()
        }

        // Cache the result with expiry
        entityChannelCache[entityId] = Pair(channels, now + CACHE_TTL_MS)

        return channels
    }

    /**
     * Get all connections subscribed to a specific channel.
     * Uses a cache with TTL to reduce database queries.
     */
    suspend fun getConnectionsForChannel(channelId: String): Set<String> {
        val now = System.currentTimeMillis()

        // Check cache first
        val cachedEntry = channelConnectionCache[channelId]
        if (cachedEntry != null) {
            val (connections, expiry) = cachedEntry
            if (now < expiry) {
                return connections
            }
            channelConnectionCache.remove(channelId)
        }

        // Query database
        val connections = try {
            channelStore.getClientIds(channelId).toSet()
        } catch (e: Exception) {
            vlog.logVerticleError("CLIENT_LOOKUP", e, mapOf(
                "channelId" to channelId
            ))
            emptySet()
        }

        // Cache the result with expiry
        channelConnectionCache[channelId] = Pair(connections, now + CACHE_TTL_MS)

        return connections
    }

    /**
     * Queue an entity update for a specific connection.
     * If the entity already has a pending update, it will be replaced with the newer version.
     */
    suspend fun queueUpdateForConnection(connectionId: String, entityId: String, entityUpdate: JsonObject) {
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