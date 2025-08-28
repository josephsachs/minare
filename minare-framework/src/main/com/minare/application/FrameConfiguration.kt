package com.minare.application

/**
 * Configuration for frame-based processing in Minare.
 *
 * This configuration defines the temporal structure of the system,
 * including frame duration, checkpointing intervals, and synchronization
 * parameters.
 *
 * TODO: Finetune, remove or simplify defaults
 */
data class FrameConfiguration(
    /**
     * Duration of each frame in milliseconds.
     * This is the fundamental time unit for the system.
     *
     * Default: 100ms (10 frames per second)
     * Production typical: 100-1000ms
     * High-frequency games: 16-33ms (30-60 FPS)
     */
    val frameDurationMs: Long = 100,

    /**
     * Number of frames between state checkpoints.
     * Checkpoints allow recovery without replaying entire history.
     *
     * Default: 600 frames (60 seconds at 100ms frames)
     * Trade-off: More frequent = faster recovery but more storage
     */
    val saveIntervalFrames: Int = 600,

    /**
     * Offset from announcement to actual frame start.
     * Gives workers time to prepare after frame announcement.
     *
     * Default: 2000ms
     * Should be > network latency + worker prep time
     */
    val frameOffsetMs: Long = 2000,

    /**
     * How long to wait for workers to complete a frame.
     * Expressed as a multiple of frame duration.
     *
     * Default: 0.8 (80% of frame duration)
     * Example: 80ms wait for 100ms frames
     */
    val coordinationWaitPeriod: Double = 0.8,

    /**
     * Interval between time synchronization checks.
     * NTP sync helps maintain temporal alignment across cluster.
     *
     * Formula: max(1, saveIntervalFrames / 4)
     * Ensures at least 4 NTP syncs between checkpoints
     */
    val timeSyncIntervalFrames: Int = 100,

    /**
     * Startup offset for frame synchronization across instances.
     * Gives all instances time to sync time and report ready.
     *
     * Default: 5000ms
     * Should accommodate typical NTP sync + network coordination
     */
    val frameStartupOffsetMs: Long = 5000,

    /**
     * Maximum clock drift tolerance before frame pause.
     * If time drift exceeds this threshold, system pauses for resync.
     *
     * Default: 100ms
     * Balances temporal precision vs operational stability
     */
    val maxClockDriftMs: Long = 100,

    /**
     * How many frames ahead to prepare during normal operation.
     * Provides smooth operation without excessive pre-computation.
     *
     * Default: 2 frames
     * Balances low latency with operational buffer
     */
    val normalOperationLookahead: Int = 2,

    /**
     * Number of frames to complete after backpressure activation
     * before resuming normal operation.
     * Default: 3 (30% of maxBufferFrames)
     */
    val catchupFramesBeforeResume: Int = 3
) {
    init {
        require(frameDurationMs > 0) { "Frame duration must be positive" }
        require(saveIntervalFrames > 0) { "Save interval must be positive" }
        require(frameStartupOffsetMs > 0) { "Startup offset must be positive" }
        require(frameOffsetMs > 0) { "Frame offset must be positive" }
        require(coordinationWaitPeriod > 0) { "Coordination wait period must be positive" }
        require(maxClockDriftMs > 0) { "Clock drift tolerance must be positive" }
        require(normalOperationLookahead > 0) { "Normal operation lookahead must be positive" }

        // Warn about potentially problematic configurations
        if (frameDurationMs < 5) {
            println("WARNING: Very short frame duration (${frameDurationMs}ms) may cause coordination issues")
        }

        if (coordinationWaitPeriod > 0.90) {
            println("WARNING: Very high coordination wait period (${coordinationWaitPeriod}x) may delay failure detection")
        }

        if (saveIntervalFrames > 100) {
            println("WARNING: Very long save interval (${saveIntervalFrames} frames) increases recovery time")
        }

        if (maxClockDriftMs > frameDurationMs) {
            println("WARNING: Max clock drift exceeds frame length. This may result in incorrect processing order.")
        }

        if (normalOperationLookahead > 5) {
            println("WARNING: Large lookahead (${normalOperationLookahead} frames) reduces operation processing responsiveness")
        }
    }
}