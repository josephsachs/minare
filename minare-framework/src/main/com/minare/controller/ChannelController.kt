package com.minare.controller

import com.minare.cache.ConnectionCache
import com.minare.core.entity.models.Entity
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ContextStore
import com.minare.core.transport.downsocket.DownSocketVerticle
import com.minare.core.transport.downsocket.DownSocketVerticle.Companion.ADDRESS_BROADCAST_CHANNEL
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.Type
import com.minare.core.utils.vertx.EventBusUtils
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Controller responsible for managing entity-channel relationships
 */
@Singleton
open class ChannelController @Inject constructor() {
    @Inject private lateinit var channelStore: ChannelStore
    @Inject private lateinit var contextStore: ContextStore
    @Inject private lateinit var eventBusUtils: EventBusUtils
    @Inject private lateinit var debug: DebugLogger

    private val log = LoggerFactory.getLogger(ChannelController::class.java)

    /**
     * Add an entity to a channel
     *
     * @param entity The entity to add
     * @param channelId The channel ID
     * @return True if successfully added, false otherwise
     */
    open suspend fun addEntityToChannel(entity: Entity, channelId: String): Boolean {
        return entity._id?.let { entityId ->
            try {
                val contextId = contextStore.createContext(entityId, channelId)
                debug.log(Type.CHANNEL_CONTROLLER_ADD_ENTITY_CHANNEL, listOf(entity._id!!, channelId, contextId))
                true
            } catch (e: Exception) {
                log.error("Failed to add entity ${entity._id} to channel $channelId", e)
                false
            }
        } ?: false
    }

    /**
     * Remove an entity from a channel
     *
     * @param entity The entity to remove
     * @param channelId The channel ID
     * @return True if successfully removed, false otherwise
     */
    open suspend fun removeEntityFromChannel(entity: Entity, channelId: String): Boolean {
        return entity._id?.let { entityId ->
            contextStore.removeContext(entityId, channelId)
        } ?: false
    }

    /**
     * Add multiple entities to a channel
     *
     * @param entities The entities to add
     * @param channelId The channel ID
     * @return The number of entities successfully added
     */
    open suspend fun addEntitiesToChannel(entities: List<Entity>, channelId: String): Int {
        var successCount = 0

        for (entity in entities) {
            if (addEntityToChannel(entity, channelId)) {
                successCount++
            }
        }

        debug.log(Type.CHANNEL_CONTROLLER_ADD_ENTITIES_CHANNEL, listOf(successCount, entities.size, channelId))
        return successCount
    }

    /**
     * Create a new channel
     *
     * @return The ID of the created channel
     */
    open suspend fun createChannel(): String {
        return channelStore.createChannel()
    }

    /**
     * Subscribe a client to a channel
     *
     * @param connectionId The client connection ID
     * @param channelId The channel ID
     * @return True if subscription was successful
     */
    open suspend fun subscribeClientToChannel(connectionId: String, channelId: String): Boolean {
        return try {
            channelStore.addClientToChannel(channelId, connectionId)
            debug.log(Type.CHANNEL_CONTROLLER_ADD_CLIENT_CHANNEL, listOf(connectionId, channelId))
            true
        } catch (e: Exception) {
            log.error("Failed to subscribe client {} to channel {}", connectionId, channelId, e)
            false
        }
    }

    /**
     * Get all entity IDs in a channel
     *
     * @param channelId The channel ID
     * @return List of entity IDs in the channel
     */
    open suspend fun getEntityIdsInChannel(channelId: String): List<String> {
        return contextStore.getEntityIdsByChannel(channelId)
    }

    /**
     * Get all connections in a channel
     */
    open suspend fun broadcastToChannel(channelId: String, message: JsonObject) {
        eventBusUtils.publishWithTracing(ADDRESS_BROADCAST_CHANNEL,
            JsonObject()
                .put("channelId", channelId)
                .put("message", message)
        )
    }
}