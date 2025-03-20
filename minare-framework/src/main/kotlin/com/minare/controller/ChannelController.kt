package com.minare.example.controller

import com.minare.core.models.Entity
import com.minare.persistence.ChannelStore
import com.minare.persistence.ContextStore
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Controller responsible for managing entity-channel relationships
 */
@Singleton
open class ChannelController @Inject constructor(
    private val channelStore: ChannelStore,
    private val contextStore: ContextStore
) {
    private val log = LoggerFactory.getLogger(ChannelController::class.java)

    /**
     * Add an entity to a channel
     *
     * @param entity The entity to add
     * @param channelId The channel ID
     * @return True if successfully added, false otherwise
     */
    suspend fun addEntityToChannel(entity: Entity, channelId: String): Boolean {
        return entity._id?.let { entityId ->
            try {
                val contextId = contextStore.createContext(entityId, channelId)
                log.debug("Added entity ${entity._id} to channel $channelId with context $contextId")
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
    suspend fun removeEntityFromChannel(entity: Entity, channelId: String): Boolean {
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
    suspend fun addEntitiesToChannel(entities: List<Entity>, channelId: String): Int {
        var successCount = 0

        for (entity in entities) {
            if (addEntityToChannel(entity, channelId)) {
                successCount++
            }
        }

        log.info("Added $successCount out of ${entities.size} entities to channel $channelId")
        return successCount
    }

    /**
     * Create a new channel
     *
     * @return The ID of the created channel
     */
    suspend fun createChannel(): String {
        return channelStore.createChannel()
    }

    /**
     * Subscribe a client to a channel
     *
     * @param connectionId The client connection ID
     * @param channelId The channel ID
     * @return True if subscription was successful
     */
    suspend fun subscribeClientToChannel(connectionId: String, channelId: String): Boolean {
        return try {
            channelStore.addClientToChannel(connectionId, channelId)
            log.info("Client {} subscribed to channel {}", connectionId, channelId)
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
    suspend fun getEntityIdsInChannel(channelId: String): List<String> {
        return contextStore.getEntityIdsByChannel(channelId)
    }
}