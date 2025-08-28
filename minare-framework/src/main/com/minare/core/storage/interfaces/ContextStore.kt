package com.minare.core.storage.interfaces

interface ContextStore {
    /**
     * Creates a context linking an entity to a channel
     * @param entityId The entity ID
     * @param channelId The channel ID
     * @return The generated context ID
     */
    suspend fun createContext(entityId: String, channelId: String): String

    /**
     * Removes a context
     * @param entityId The entity ID
     * @param channelId The channel ID
     * @return A Future indicating success or failure
     */
    suspend fun removeContext(entityId: String, channelId: String): Boolean

    /**
     * Gets all channel IDs associated with an entity
     * @param entityId The entity ID
     * @return A list of channel IDs
     */
    suspend fun getChannelsByEntityId(entityId: String): List<String>

    /**
     * Gets all entity IDs associated with a channel
     * @param channelId The channel ID
     * @return A list of entity IDs
     */
    suspend fun getEntityIdsByChannel(channelId: String): List<String>
}