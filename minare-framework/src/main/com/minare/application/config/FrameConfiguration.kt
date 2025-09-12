package com.minare.application.config

import java.awt.Frame

/**
 * Configuration for frame-based processing in Minare.
 *
 * This configuration defines the temporal structure of the system,
 * including frame duration, session intervals, and timing
 * parameters. Overridable by application.
 *
 */
open class FrameConfiguration {    /**
     * Duration of each frame in milliseconds.
     * This is the fundamental time unit for the system.
     *
     * Default: 100ms (10 frames per second)
     * Trade-off: Faster = faster response and more reliable ordering but
     *     more processor overhead
     */
    val frameDurationMs: Long = 100

    /**
     * Number of frames between automatic sessions.
     * Sessions save snapshots, clear coordination memory
     * and and reset frame number
     *
     * Default: 1000 frames
     * Trade-off: More frequent = faster recovery but more frequent pauses
     */
    val autoSession: AutoSession = AutoSession.FRAMES_PER_SESSION
    val framesPerSession: Long = 500

    /**
     * How many frames ahead to prepare during normal operation.
     * Provides smooth operation without excessive pre-computation.
     *
     * Default: 2 frames
     * Balances low latency with operational buffer
     */
    val normalOperationLookahead: Int = 2

    companion object {
        enum class AutoSession {
            NONE,
            FRAMES_PER_SESSION
        }
    }
}
