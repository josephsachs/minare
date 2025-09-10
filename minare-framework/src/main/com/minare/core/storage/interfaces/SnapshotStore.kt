package com.minare.core.storage.interfaces

import com.minare.core.frames.models.FrameDelta
import io.vertx.core.json.JsonObject

/**
 * Interface for snapshot storage operations.
 * Handles session snapshots for recovery.
 */
interface SnapshotStore {
    /**
     * Create a snapshot collection by sessionId
     * @param sessionId The ID of the collection to create
     */
    suspend fun create(sessionId: String, metadata: JsonObject)

    /**
     * Store deltas under the appropriate collection
     * @param sessionId The ID of the session to store in
     * @param deltas A set of FrameDelta to store
     */
    suspend fun storeDeltas(sessionId: String, deltas: List<FrameDelta>)

    /**
     * Store entity state from a worker
     * @param sessionId The ID of the session to store in
     * @param entities List of JsonObjects
     */
    suspend fun storeState(sessionId: String, entities: List<JsonObject>)

    /**
     * Check if a snapshot exists for a session
     * @param sessionId The ID of the session to store in
     * @return Boolean
     */
    suspend fun exists(sessionId: String): Boolean

    /**
     * Get metadata for a snapshot
     * @param sessionId
     * @return JsonObject?
     */
    suspend fun getMetadata(sessionId: String): JsonObject?

    /**
     * Get deltas from a snapshot
     * @param sessionId
     * @return List<FrameDelta>
     */
    suspend fun getDeltas(sessionId: String): List<FrameDelta>
}