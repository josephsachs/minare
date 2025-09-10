package com.minare.core.storage.interfaces

import com.minare.core.frames.models.FrameDelta

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
    suspend fun getFrameDeltas(frameNumber: Long): List<FrameDelta>

    /**
     * Retrieve deltas for a range of frames
     * @param startFrame Starting frame number (inclusive)
     * @param endFrame Ending frame number (inclusive)
     * @return List of deltas sorted by frame number and operation order
     */
    suspend fun getDeltasForFrameRange(startFrame: Long, endFrame: Long): List<FrameDelta>

    /**
     * Retrieve deltas for specific entities across all frames
     * @param entityIds Set of entity IDs to filter by
     * @return List of deltas for those entities, sorted by frame/version
     */
    suspend fun getDeltasForEntities(entityIds: Set<String>): List<FrameDelta>

    /**
     * Clear all stored deltas (typically after successful snapshot)
     */
    suspend fun clearAllDeltas()
}