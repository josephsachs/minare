package com.minare.application.config

/**
 * Configuration for frame-based processing in Minare.
 *
 * This configuration defines the temporal structure of the system,
 * including frame duration, session intervals, and timing
 * parameters. Overridable by application.
 *
 */
open class FrameConfiguration {
    /**
     *
     *     Frame settings
     *
     */

    /**
     * Duration of each frame in milliseconds.
     * This is the fundamental time unit for the system.
     *
     * Default: 100ms (10 frames per second)
     * Trade-off: Faster = faster response and more reliable ordering but
     *     more processor overhead
     */
    open val frameDurationMs: Long = 500

    /**
     * How many frames ahead to prepare during normal operation.
     * Provides smooth operation without excessive pre-computation.
     *
     * Default: 2 frames
     * Balances low latency with operational buffer
     */
    open val normalOperationLookahead: Int = 250

    /**
     *
     *    Session settings
     *
     */

    /**
     * Number of frames between automatic sessions.
     * Sessions save snapshots, clear coordination memory
     * and reset frame number
     *
     * Default: 1000 frames
     * Trade-off: More frequent = faster recovery but more frequent pauses
     */
    open val framesPerSession: Long = 150

    /**
     *
     *    Timeline settings
     *
     */

    /**
     * Allow frame manifests to complete processing before hard pause
     */
    open val flushOperationsOnDetach: Boolean = true

    /**
     * Detach uses soft pause, buffering new input
     */
    open val bufferInputDuringDetach: Boolean = true

    /**
     * Replay uses soft pause, buffering new input
     */
    open val bufferInputDuringReplay: Boolean = true

    /**
     * Resume assigns operations from stale frames to new session,
     * preserving temporal order
     */
    open val assignOperationsOnResume: Boolean = false

    /**
     * Resume replays to current frameInProgress before returning play to State
     */
    open val replayOnResume: Boolean = false


}
