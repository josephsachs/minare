package com.minare.persistence

import com.google.inject.Inject
import com.google.inject.name.Named
import com.minare.persistence.ChannelStore
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.await
import java.util.*

class MongoChannelStore @Inject constructor(
    private val mongoClient: MongoClient
) : ChannelStore {
    private val collection = "channels"

    /**
     * Creates a new channel
     * @return The generated channel ID
     */
    override suspend fun createChannel(): String {
        val document = JsonObject()
            .put("clients", JsonArray())
            .put("created", System.currentTimeMillis())

        val result = mongoClient.insert(collection, document).await()

        return result
    }

    /**
     * Adds a client to a channel
     * @param channelId The channel ID
     * @param clientId The client ID to add
     * @return Boolean indicating success or failure
     */
    override suspend fun addClientToChannel(channelId: String, clientId: String): Boolean {
        val query = JsonObject().put("_id", channelId)
        val update = JsonObject().put("\$addToSet", JsonObject().put("clients", clientId))

        val result = mongoClient.updateCollection(collection, query, update).await()

        return result.docMatched > 0
    }

    /**
     * Removes a client from a channel
     * @param channelId The channel ID
     * @param clientId The client ID to remove
     * @return Boolean indicating success or failure
     */
    override suspend fun removeClientFromChannel(channelId: String, clientId: String): Boolean {
        val query = JsonObject().put("_id", channelId)
        val update = JsonObject().put("\$pull", JsonObject().put("clients", clientId))

        val result = mongoClient.updateCollection(collection, query, update).await()

        return result.docMatched > 0
    }

    /**
     * Gets a channel by ID
     * @param channelId The channel ID to retrieve
     * @return A JsonObject representing the channel, or null if not found
     */
    override suspend fun getChannel(channelId: String): JsonObject? {
        val query = JsonObject().put("_id", channelId)

        return mongoClient.findOne(collection, query, null).await()
    }

    /**
     * Gets all client IDs associated with a channel
     * @param channelId The channel ID
     * @return A list of client IDs
     */
    override suspend fun getClientIds(channelId: String): List<String> {
        val channel = getChannel(channelId) ?: return emptyList()
        val clientsArray = channel.getJsonArray("clients", JsonArray())

        return clientsArray.map { it.toString() }
    }

    /**
     * Gets all channel IDs that a client is subscribed to
     * @param clientId The client ID
     * @return A list of channel IDs
     */
    override suspend fun getChannelsForClient(clientId: String): List<String> {
        val query = JsonObject().put("clients", clientId)
        val results = mongoClient.find(collection, query).await()

        return results.map { it.getString("_id") }
    }
}