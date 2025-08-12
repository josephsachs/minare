package com.minare.time

/**
 * Configuration for frame-based processing in Minare.
 *
 * This configuration defines the temporal structure of the system,
 * including frame duration, checkpointing intervals, and synchronization
 * parameters.
 *
 * Updated with event-driven coordination constants.
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
     * Maximum frames to buffer during pause conditions.
     * When paused, operations continue arriving and are buffered
     * up to this many frames ahead.
     *
     * Default: 10 frames (1 second at 100ms frames)
     * After this, backpressure (503) should be applied
     */
    val maxBufferFrames: Int = 10,

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
        require(maxBufferFrames > 0) { "Max buffer frames must be positive" }
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

        if (maxBufferFrames > 50) {
            println("WARNING: Very large buffer (${maxBufferFrames} frames) may cause memory issues")
        }

        if (normalOperationLookahead > 5) {
            println("WARNING: Large lookahead (${normalOperationLookahead} frames) reduces operation processing responsiveness")
        }
    }

    /**
     * Create a configuration suitable for high-frequency games.
     * Short frames with frequent checkpoints.
     */
    companion object {
        fun highFrequencyGame() = FrameConfiguration(
            frameDurationMs = 16,          // 60 FPS
            saveIntervalFrames = 1800,     // Every 30 seconds
            frameOffsetMs = 500,           // Quick start
            coordinationWaitPeriod = 0.5,  // Tight timing
            maxBufferFrames = 60,          // 1 second buffer
            normalOperationLookahead = 3   // Slightly more buffer for smooth gameplay
        )

        /**
         * Create a configuration for batch processing systems.
         * Longer frames with relaxed timing.
         */
        fun batchProcessing() = FrameConfiguration(
            frameDurationMs = 1000,        // 1 second frames
            saveIntervalFrames = 60,       // Every minute
            frameOffsetMs = 5000,          // Plenty of prep time
            coordinationWaitPeriod = 0.9,  // Relaxed timing
            maxBufferFrames = 30,          // 30 second buffer
            normalOperationLookahead = 1   // Minimal lookahead
        )

        /**
         * Create a configuration for real-time analytics.
         * Balanced for low latency with reliability.
         */
        fun realtimeAnalytics() = FrameConfiguration(
            frameDurationMs = 100,         // 10 FPS
            saveIntervalFrames = 600,      // Every minute
            frameOffsetMs = 2000,          // Standard prep
            coordinationWaitPeriod = 0.8,  // Standard timing
            maxBufferFrames = 20,          // 2 second buffer
            normalOperationLookahead = 2   // Standard lookahead
        )
    }
}