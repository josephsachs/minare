package com.minare.core.storage.adapters

import com.google.inject.Inject
import com.minare.core.frames.models.FrameDelta
import com.minare.core.storage.interfaces.SnapshotStore
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.await
import javax.inject.Singleton

/**
 * MongoDB implementation of SnapshotStore
 */
@Singleton
class MongoSnapshotStore @Inject constructor(
    private val mongoClient: MongoClient
) : SnapshotStore {
    private val log = LoggerFactory.getLogger(MongoSnapshotStore::class.java)

    companion object {
        const val METADATA_DOC_ID = "metadata"
        const val DELTAS_DOC_ID = "deltas"
        const val STATE_DOC_ID = "state"
    }

    /**
     * Get the collection name for a session's snapshot
     */
    private fun getCollectionName(sessionId: String): String {
        return "snapshot_$sessionId"
    }
    /**
     * Create a snapshot collection by sessionId
     * @param sessionId The ID of the collection to create
     */
    override suspend fun create(sessionId: String, metadata: JsonObject) {
        val collection = getCollectionName(sessionId)

        try {
            // Store metadata document
            val metadataDoc = JsonObject()
                .put("_id", METADATA_DOC_ID)
                .put("sessionId", sessionId)
                .put("createdAt", System.currentTimeMillis())
                .put("sessionStartTimestamp", metadata.getLong("sessionStartTimestamp"))
                .put("announcementTimestamp", metadata.getLong("announcementTimestamp"))
                .put("frameDuration", metadata.getLong("frameDuration"))
                .put("workerCount", metadata.getInteger("workerCount"))
                .put("workerIds", metadata.getJsonArray("workerIds"))
                .put("coordinatorInstance", metadata.getString("coordinatorInstance"))
                .mergeIn(metadata)

            mongoClient.save(collection, metadataDoc).await()

            // Initialize documents
            val deltasDoc = JsonObject()
                .put("_id", DELTAS_DOC_ID)
                .put("deltas", JsonArray())

            val stateDoc = JsonObject()
                .put("_id", STATE_DOC_ID)
                .put("state", JsonArray())

            mongoClient.save(collection, deltasDoc).await()
            mongoClient.save(collection, stateDoc).await()

            log.info("Created snapshot collection for session: $sessionId", )
        } catch (e: Exception) {
            log.error("Failed to create snapshot for session: $sessionId ${e}")
            throw e
        }
    }

    /**
     * Store deltas under the appropriate collection
     * @param sessionId The ID of the session to store in
     * @param deltas A set of FrameDelta to store
     */
    override suspend fun storeDeltas(sessionId: String, deltas: JsonObject) {
        val collection = getCollectionName(sessionId)

        try {
            val query = JsonObject().put("_id", DELTAS_DOC_ID)
            val deltasDoc = JsonObject()
                .put("\$set", JsonObject()
                    .put("frames", deltas)
                    .put("deltaCount", deltas.size())
                    .put("updatedAt", System.currentTimeMillis())
                )

            mongoClient.updateCollection(collection, query, deltasDoc).await()

            log.info("Stored deltas for session: $sessionId")
        } catch (e: Exception) {
            log.error("Failed to store deltas for session: $sessionId ${e}")
            throw e
        }
    }

    /**
     * Store entity state from a worker
     * @param sessionId The ID of the session to store in
     * @param entities List of JsonObjects
     */
    override suspend fun storeState(sessionId: String, entities: JsonArray) {
        val collection = getCollectionName(sessionId)

        try {
            val entitiesArray = JsonArray()
            entities.forEach { entity ->
                entitiesArray.add(entity)
            }

            val query = JsonObject().put("_id", STATE_DOC_ID)
            val stateDoc = JsonObject()
                .put("\$push", JsonObject()
                    .put("state", JsonObject()
                        .put("\$each", entitiesArray)
                    )
                )
                .put("\$inc", JsonObject()
                    .put("entityCount", entities.size())
                )
                .put("\$set", JsonObject()
                    .put("lastUpdatedAt", System.currentTimeMillis())
                )

            mongoClient.updateCollection(collection, query, stateDoc).await()

            log.info("Stored ${entities.size()} entities for session: $sessionId")
        } catch (e: Exception) {
            log.error("Failed to store state for session: $sessionId ${e}")
            throw e
        }
    }

    /**
     * Check if a snapshot exists for a session
     * @param sessionId The ID of the session to store in
     * @return Boolean
     */
    override suspend fun exists(sessionId: String): Boolean {
        val collection = getCollectionName(sessionId)
        val query = JsonObject().put("_id", METADATA_DOC_ID)

        try {
            val count = mongoClient.count(collection, query).await()
            return count > 0
        } catch (e: Exception) {
            log.debug("Error checking if snapshot exists for session: $sessionId ${e}")
            return false
        }
    }

    /**
     * Get metadata for a snapshot
     * @param sessionId
     * @return JsonObject?
     */
    override suspend fun getMetadata(sessionId: String): JsonObject? {
        val collection = getCollectionName(sessionId)
        val query = JsonObject().put("_id", METADATA_DOC_ID)

        try {
            val result = mongoClient.findOne(collection, query, null).await()

            if (result == null) {
                log.debug("Collection snapshot_${sessionId} not found in session store")
                return null
            }

            return result
        } catch (e: Exception) {
            log.error("Error retrieving metadata for session: $sessionId ${e}")
            throw e
        }
    }

    /**
     * Get deltas from a snapshot
     * @param sessionId
     * @return List<FrameDelta>
     */
    override suspend fun getDeltas(sessionId: String): List<FrameDelta> {
        val collection = getCollectionName(sessionId)
        val query = JsonObject().put("_id", DELTAS_DOC_ID)

        try {
            val result = mongoClient.findOne(collection, query, null).await()

            if (result == null) {
                log.debug("Deltas not found for session: $sessionId")
                return emptyList()
            }

            val deltasArray = result.getJsonArray("deltas", JsonArray())
            return deltasArray.map { deltaJson ->
                FrameDelta.fromJson(deltaJson as JsonObject)
            }
        } catch (e: Exception) {
            log.error("Error retrieving deltas for session: $sessionId ${e}")
            throw e
        }
    }
}