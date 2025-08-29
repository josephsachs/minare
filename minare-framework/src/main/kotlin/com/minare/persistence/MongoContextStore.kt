package com.minare.persistence

import com.google.inject.Inject
import com.google.inject.name.Named
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory
import java.util.*
import javax.inject.Singleton

@Singleton
class MongoContextStore @Inject constructor(
    private val mongoClient: MongoClient,
    @Named("contexts") private val collection: String
) : ContextStore {
    private val log = LoggerFactory.getLogger(MongoContextStore::class.java)

    /**
     * Creates a context linking an entity to a channel
     * @param entityId The entity ID
     * @param channelId The channel ID
     * @return The generated context ID
     */
    override suspend fun createContext(entityId: String, channelId: String): String {
        if (entityId.isBlank()) {
            throw IllegalArgumentException("Entity ID cannot be blank")
        }

        if (channelId.isBlank()) {
            throw IllegalArgumentException("Channel ID cannot be blank")
        }

        val contextId = "$entityId-$channelId"

        val document = JsonObject()
            .put("_id", contextId)
            .put("entity", entityId)
            .put("channel", channelId)
            .put("created", System.currentTimeMillis())

        log.debug("Creating context with ID: $contextId for entity: $entityId and channel: $channelId")

        try {
            if (mongoClient == null) {
                log.error("MongoClient is null")
                throw NullPointerException("MongoClient is null")
            }

            // When using replica sets, the insert operation may return null even on success
            // This is due to write concern behavior in replica sets
            val result = mongoClient.insert(collection, document).await()

            // Handle the case where result is null (expected with replica sets)
            if (result == null) {
                log.info("MongoDB insert operation completed but returned null ID for contextId: $contextId")

                // Verify the document was actually inserted
                val query = JsonObject().put("_id", contextId)
                val exists = mongoClient.findOne(collection, query, JsonObject()).await() != null

                if (exists) {
                    log.debug("Verified document exists in collection after insert")
                } else {
                    log.warn("Document appears to not exist after insert operation - replica set may be syncing")
                }

                // Return the predefined contextId since we set it explicitly
                return contextId
            }

            log.debug("Context created successfully with returned ID: $result")
            return result
        } catch (e: Exception) {
            if (e.message?.contains("duplicate key") == true) {
                log.debug("Context already exists: $contextId")
                return contextId
            }
            log.error("Failed to create context", e)
            throw e
        }
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