package com.minare.core.storage.interfaces

import com.minare.core.frames.models.FrameDelta
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

/**
 * Interface for delta storage operations.
 * Handles frame-by-frame delta persistence for replay and snapshots.
 */
interface DeltaStore {
    /**
     * Append a delta to a frame's delta list
     * @param frameNumber The logical frame number
     * @param delta The delta to store
     */
    suspend fun appendDelta(frameNumber: Long, delta: FrameDelta)

    /**
     * Retrieve all deltas for a specific frame
     * @param frameNumber The logical frame number
     * @return List of deltas for that frame, in order
     */
    suspend fun getByFrame(frameNumber: Long): JsonObject?

    /**
     * Retrieve all deltas as a Json object
     * @return JsonObject matching the JsonRL structure
     */
    suspend fun getAll(): JsonObject

    /**
     * Clear all stored deltas (typically after successful snapshot)
     */
    suspend fun clearDeltas()
}