package com.minare.time

import javax.inject.Inject
import javax.inject.Singleton

/**
 * Configuration for the distributed frame controller system.
 *
 * Provides user-configurable settings with sensible defaults following
 * convention-over-configuration principles. Framework derives timing
 * settings based on user preferences.
 *
 * Immutable once set - changes require application restart.
 */
data class FrameConfiguration constructor(
    /**
     * Duration of each frame in milliseconds.
     * All commands within this time window are processed together.
     *
     * Default: 1000ms (1 second frames)
     * Suitable for: Turn-based games, slow real-time games
     *
     * Consider shorter durations for:
     * - Fast-paced action games (100-500ms)
     * - High-frequency trading systems (10-100ms)
     */
    val frameDurationMs: Long = 10000,

    /**
     * Gap between frames for coordination and pause/resume operations.
     * Occurs after postFrame() hook, before preFrame() hook
     *
     * Default: 100ms
     * Should allow for:
     * - Inter-frame coordination messages
     * - Instance status reporting
     * - Brief pause/resume cycles
     * - Network latency buffer
     */
    val frameOffsetMs: Long = 1000,

    /**
     * Coordination timeout for failure detection.
     * Expressed as a percentage of the offset time.
     *
     * Default: 0.8 (80% interframe time used)
     */
    val coordinationWaitPeriod: Double = 0.8,

    /**
     * Number of frames between database checkpoint saves.
     * Used for disaster recovery via write-behind persistence.
     * Occurs after postFrame() and before offset.
     *
     * Default: 10 frames
     * At 1000ms frames: checkpoint every 10 seconds
     * At 100ms frames: checkpoint every 1 second
     */
    val saveIntervalFrames: Int = 100,

    /**
     * Framework-derived: time synchronization frequency.
     * Based on checkpoint frequency to maintain time precision
     * relative to user's consistency/performance preferences.
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
    val frameStartupOffsetMs: Long = 50000,

    /**
     * Maximum clock drift tolerance before frame pause.
     * If time drift exceeds this threshold, system pauses for resync.
     *
     * Default: 100ms
     * Balances temporal precision vs operational stability
     */
    val maxClockDriftMs: Long = 1000
) {
    init {
        require(frameDurationMs > 0) { "Frame duration must be positive" }
        require(saveIntervalFrames > 0) { "Save interval must be positive" }
        require(frameStartupOffsetMs > 0) { "Startup offset must be positive" }
        require(frameOffsetMs > 0) { "Frame offset must be positive" }
        require(coordinationWaitPeriod > 0) { "Coordination wait period must be positive" }
        require(maxClockDriftMs > 0) { "Clock drift tolerance must be positive" }

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
    }

    /**
     * Create a configuration suitable for high-frequency games.
     * Short frames with frequent checkpoints.
     */
    companion object {
        fun forHighFrequencyGame() = FrameConfiguration(
            frameDurationMs = 16,              // ~60 FPS gaming standard
            frameOffsetMs = 4,                 // 25% of frame for coordination
            coordinationWaitPeriod = 0.75,     // Tight failure detection
            saveIntervalFrames = 200,          // Save every ~3.2 seconds
            timeSyncIntervalFrames = 50,       // Sync every ~800ms
            frameStartupOffsetMs = 2000,       // Quick startup
            maxClockDriftMs = 8                // Half frame tolerance
        )

        fun forTurnBasedGame() = FrameConfiguration(
            frameDurationMs = 2000,            // 2 second turns
            frameOffsetMs = 200,               // 10% for coordination
            coordinationWaitPeriod = 0.9,      // Generous failure detection
            saveIntervalFrames = 30,           // Save every minute
            timeSyncIntervalFrames = 10,       // Sync every 20 seconds
            frameStartupOffsetMs = 10000,      // Allow slow startup
            maxClockDriftMs = 500              // Relaxed drift tolerance
        )

        fun forRealTimeStrategy() = FrameConfiguration(
            frameDurationMs = 200,             // 5 FPS simulation tick
            frameOffsetMs = 20,                // 10% for coordination
            coordinationWaitPeriod = 0.8,      // Balanced failure detection
            saveIntervalFrames = 150,          // Save every 30 seconds
            timeSyncIntervalFrames = 75,       // Sync every 15 seconds
            frameStartupOffsetMs = 5000,       // Standard startup
            maxClockDriftMs = 50               // Moderate drift tolerance
        )
    }
}