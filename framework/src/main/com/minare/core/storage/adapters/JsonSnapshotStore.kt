package com.minare.core.storage.adapters

import com.google.inject.Inject
import com.minare.application.config.FrameworkConfig
import com.minare.core.frames.models.FrameDelta
import com.minare.core.storage.interfaces.SnapshotStore
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import javax.inject.Singleton

/**
 * MongoDB implementation of SnapshotStore
 */
@Singleton
class JsonSnapshotStore @Inject constructor(
    private val frameworkConfig: FrameworkConfig
) : SnapshotStore {

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
        throw Exception("Not implemented")
    }

    /**
     * Store deltas under the appropriate collection
     * @param sessionId The ID of the session to store in
     * @param deltas A set of FrameDelta to store
     */
    override suspend fun storeDeltas(sessionId: String, deltas: JsonObject) {
        throw Exception("Not implemented")
    }

    /**
     * Store entity state from a worker
     * @param sessionId The ID of the session to store in
     * @param entities List of JsonObjects
     */
    override suspend fun storeState(sessionId: String, entities: JsonArray) {
        throw Exception("Not implemented")
    }

    /**
     * Check if a snapshot exists for a session
     * @param sessionId The ID of the session to store in
     * @return Boolean
     */
    override suspend fun exists(sessionId: String): Boolean {
        throw Exception("Not implemented")
    }

    /**
     * Get metadata for a snapshot
     * @param sessionId
     * @return JsonObject?
     */
    override suspend fun getMetadata(sessionId: String): JsonObject? {
        throw Exception("Not implemented")
    }

    /**
     * Get deltas from a snapshot
     * @param sessionId
     * @return List<FrameDelta>
     */
    override suspend fun getDeltas(sessionId: String): List<FrameDelta> {
        throw Exception("Not implemented")
    }
}