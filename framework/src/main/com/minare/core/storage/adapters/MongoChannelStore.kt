package com.minare.core.storage.adapters

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.ext.mongo.UpdateOptions
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory
import javax.inject.Inject
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.utils.debug.DebugLogger

class MongoChannelStore @Inject constructor(
    private val mongoClient: MongoClient,
) : ChannelStore {
    private val collection = "channels"
    private val log = LoggerFactory.getLogger(MongoChannelStore::class.java)

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
    override suspend fun addChannelClient(channelId: String, clientId: String): Boolean {
        try {
            val query = JsonObject().put("_id", JsonObject().put("\$oid", channelId))
            val update = JsonObject().put("\$addToSet", JsonObject().put("clients", clientId))

            val result = mongoClient.updateCollection(collection, query, update).await()

            if (result.docMatched == 0L) {
                log.warn("No channel found with ID: $channelId")
                return false
            }

            return result.docModified > 0
        } catch (e: Exception) {
            log.error("Failed to add client $clientId to channel $channelId", e)
            return false
        }
    }

    /**
     * Removes a client from a channel - doesn't care if client exists
     * @param channelId The channel ID
     * @param clientId The client ID to remove
     * @return Boolean indicating success or failure
     */
    override suspend fun removeChannelClient(channelId: String, clientId: String): Boolean {
        try {
            val query = JsonObject().put("_id", JsonObject().put("\$oid", channelId))
            val update = JsonObject().put("\$pull", JsonObject().put("clients", clientId))

            val result = mongoClient.updateCollection(collection, query, update).await()

            if (result.docMatched == 0L) {
                log.warn("No channel found with ID: $channelId when removing client")
                return false
            }

            return result.docModified > 0
        } catch (e: Exception) {
            log.error("Failed to remove client $clientId from channel $channelId", e)
            return false
        }
    }

    /**
     * Removes a client from all channels - doesn't care if client exists
     * @param clientId The client ID to remove
     * @return Number of channels the client was removed from
     */
    override suspend fun removeClientFromAllChannels(clientId: String): Int {
        try {
            val query = JsonObject().put("clients", clientId)
            val update = JsonObject().put("\$pull", JsonObject().put("clients", clientId))

            val result = mongoClient
                .updateCollectionWithOptions(collection, query, update,
                    UpdateOptions().setMulti(true) // Update all matching documents
                )
                .await()

            return result.docModified.toInt()
        } catch (e: Exception) {
            log.error("Failed to remove client $clientId from all channels", e)
            return 0
        }
    }

    /**
     * Gets a channel by ID
     * @param channelId The channel ID to retrieve
     * @return A JsonObject representing the channel, or null if not found
     */
    override suspend fun getChannel(channelId: String): JsonObject? {
        try {
            val query = JsonObject().put("_id", JsonObject().put("\$oid", channelId))

            return mongoClient.findOne(collection, query, null).await()
        } catch (e: Exception) {
            log.error("Failed to get channel $channelId", e)
            return null
        }
    }

    /**
     * Gets all client IDs associated with a channel
     * @param channelId The channel ID
     * @return A list of client IDs
     */
    override suspend fun getClientIds(channelId: String): List<String> {
        try {
            val channel = getChannel(channelId)

            if (channel == null) {
                log.warn("No channel found with ID: $channelId when getting client IDs")
                return emptyList()
            }

            val clientsArray = channel.getJsonArray("clients", JsonArray())

            return clientsArray.map { it.toString() }
        } catch (e: Exception) {
            log.error("Failed to get client IDs for channel $channelId", e)
            return emptyList()
        }
    }

    /**
     * Gets all channel IDs that a client is subscribed to.
     * This method doesn't care if the client exists as a Connection.
     *
     * @param clientId The client ID
     * @return A list of channel IDs
     */
    override suspend fun getChannelsForClient(clientId: String): List<String> {
        try {
            val query = JsonObject().put("clients", clientId)

            val results = mongoClient.find(collection, query).await()

            return results.mapNotNull { document ->
                document.getString("_id")
            }
        } catch (e: Exception) {
            log.error("Failed to get channels for client $clientId", e)
            return emptyList()
        }
    }
}