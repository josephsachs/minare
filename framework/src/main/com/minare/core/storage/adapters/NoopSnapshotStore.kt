package com.minare.core.storage.adapters

import com.google.inject.Inject
import com.minare.application.config.FrameworkConfig
import com.minare.core.frames.models.FrameDelta
import com.minare.core.storage.interfaces.SnapshotStore
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import javax.inject.Singleton

/**
 * MongoDB implementation of SnapshotStore
 */
@Singleton
class NoopSnapshotStore @Inject constructor(
    private val frameworkConfig: FrameworkConfig,
    private val debug: DebugLogger
) : SnapshotStore {
    /**
     * Create a snapshot collection by sessionId
     * @param sessionId The ID of the collection to create
     */
    override suspend fun create(sessionId: String, metadata: JsonObject) {
        debug.log(DebugType.COORDINATOR_SNAPSHOT_COLLECTION_NOT_CREATED)
    }

    /**
     * Store deltas under the appropriate collection
     * @param sessionId The ID of the session to store in
     * @param deltas A set of FrameDelta to store
     */
    override suspend fun storeDeltas(sessionId: String, deltas: JsonObject) {
        debug.log(DebugType.COORDINATOR_SNAPSHOT_DELTAS_NOT_STORED)
    }

    /**
     * Store entity state from a worker
     * @param sessionId The ID of the session to store in
     * @param entities List of JsonObjects
     */
    override suspend fun storeState(sessionId: String, entities: JsonArray) {
        debug.log(DebugType.COORDINATOR_SNAPSHOT_STATE_NOT_STORED)
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