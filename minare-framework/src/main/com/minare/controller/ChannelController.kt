package com.minare.controller

import com.minare.core.entity.models.Entity
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ContextStore
import com.minare.core.transport.downsocket.DownSocketVerticle.Companion.ADDRESS_BROADCAST_CHANNEL
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.Type
import com.minare.core.utils.vertx.EventBusUtils
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Controls channel and context models, including channel subscriptions
 * and entity channel contexts, and provides message broadcast capability.
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
    open suspend fun addEntity(entity: Entity, channelId: String): Boolean {
        return entity._id?.let { entityId ->
            try {
                val contextId = contextStore.create(entityId, channelId)
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
    open suspend fun removeEntity(entity: Entity, channelId: String): Boolean {
        return entity._id?.let { entityId ->
            contextStore.remove(entityId, channelId)
        } ?: false
    }

    /**
     * Add multiple entities to a channel
     *
     * @param entities The entities to add
     * @param channelId The channel ID
     * @return The number of entities successfully added
     */
    open suspend fun addEntitiesToChannel(entities: List<Entity>, channelId: String) {
        var count: Int = 0

        for (entity in entities) {
            try {
                addEntity(entity, channelId)
                count++
            } catch (e: Exception) {
                log.error("ChannelController could not add entity ${entity} to channel \n${e}")
            }
        }

        debug.log(Type.CHANNEL_CONTROLLER_ADD_ENTITIES_CHANNEL, listOf(count, entities.size, channelId))
    }

    /**
     * Create a new channel
     *
     * @return The ID of the created channel
     */
    open suspend fun createChannel(): String {
        val result = channelStore.createChannel()

        debug.log(Type.CHANNEL_CONTROLLER_CREATE_CHANNEL, listOf(result))

        return result
    }

    /**
     * Subscribe a client to a channel
     *
     * @param connectionId The client connection ID
     * @param channelId The channel ID
     * @return True if subscription was successful
     */
    open suspend fun addClient(connectionId: String, channelId: String): Boolean {
        return try {
            channelStore.addChannelClient(channelId, connectionId)
            debug.log(Type.CHANNEL_CONTROLLER_ADD_CLIENT_CHANNEL, listOf(connectionId, channelId))
            true
        } catch (e: Exception) {
            log.error("Failed to subscribe client {} to channel {}", connectionId, channelId, e)
            false
        }
    }

    /**
     * Get all connections in a channel
     */
    open suspend fun broadcast(channelId: String, message: JsonObject) {
        eventBusUtils.publishWithTracing(ADDRESS_BROADCAST_CHANNEL,
            JsonObject()
                .put("channelId", channelId)
                .put("message", message)
        )
    }
}