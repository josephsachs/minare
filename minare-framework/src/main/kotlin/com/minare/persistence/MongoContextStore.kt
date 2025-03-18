package com.minare.persistence

import com.google.inject.Inject
import com.google.inject.name.Named
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.await
import java.util.*

class MongoContextStore @Inject constructor(
    private val mongoClient: MongoClient
) : ContextStore {
    private val collection = "contexts"

    /**
     * Creates a context linking an entity to a channel
     * @param entityId The entity ID
     * @param channelId The channel ID
     * @return The generated context ID
     */
    override suspend fun createContext(entityId: String, channelId: String): String {
        val contextId = UUID.randomUUID().toString()
        val document = JsonObject()
            .put("_id", contextId)
            .put("entity", entityId)
            .put("channel", channelId)
            .put("created", System.currentTimeMillis())

        val result = mongoClient.insert(collection, document).await()
        return result
    }

    /**
     * Removes a context
     * @param entityId The entity ID
     * @param channelId The channel ID
     * @return Boolean indicating success or failure
     */
    override suspend fun removeContext(entityId: String, channelId: String): Boolean {
        val query = JsonObject()
            .put("entity", entityId)
            .put("channel", channelId)

        val result = mongoClient.removeDocuments(collection, query).await()
        return result.removedCount > 0
    }

    /**
     * Gets all channel IDs associated with an entity
     * @param entityId The entity ID
     * @return A list of channel IDs
     */
    override suspend fun getChannelsByEntityId(entityId: String): List<String> {
        val query = JsonObject().put("entity", entityId)
        val results = mongoClient.find(collection, query).await()

        return results.mapNotNull { it.getString("channel") }
    }

    /**
     * Gets all entity IDs associated with a channel
     * @param channelId The channel ID
     * @return A list of entity IDs
     */
    override suspend fun getEntityIdsByChannel(channelId: String): List<String> {
        val query = JsonObject().put("channel", channelId)
        val results = mongoClient.find(collection, query).await()

        return results.mapNotNull { it.getString("entity") }
    }
}