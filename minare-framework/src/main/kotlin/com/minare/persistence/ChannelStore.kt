package com.minare.persistence

import io.vertx.core.json.JsonObject

interface ChannelStore {
    /**
     * Creates a new channel
     * @return The generated channel ID
     */
    suspend fun createChannel(): String

    /**
     * Adds a client to a channel
     * @param channelId The channel ID
     * @param clientId The client ID to add
     * @return A Future indicating success or failure
     */
    suspend fun addClientToChannel(channelId: String, clientId: String): Boolean

    /**
     * Removes a client from a channel
     * @param channelId The channel ID
     * @param clientId The client ID to remove
     * @return A Future indicating success or failure
     */
    suspend fun removeClientFromChannel(channelId: String, clientId: String): Boolean

    /**
     * Gets a channel by ID
     * @param channelId The channel ID to retrieve
     * @return A JsonObject representing the channel, or null if not found
     */
    suspend fun getChannel(channelId: String): JsonObject?

    /**
     * Gets all client IDs associated with a channel
     * @param channelId The channel ID
     * @return A list of client IDs
     */
    suspend fun getClientIds(channelId: String): List<String>
}