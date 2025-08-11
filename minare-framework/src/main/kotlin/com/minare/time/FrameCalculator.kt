package com.minare.time

import com.minare.time.FrameConfiguration
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Injectable utility for frame-related calculations.
 * Encapsulates frame math logic and the FrameConfiguration dependency.
 */
@Singleton
class FrameCalculator @Inject constructor(
    val frameConfig: FrameConfiguration
) {
    companion object {
        const val NANOS_PER_MS = 1_000_000L
        const val NANOS_PER_SECOND = 1_000_000_000L
    }

    // Cached for performance
    private val frameDurationNanos = frameConfig.frameDurationMs * NANOS_PER_MS

    /**
     * Convert elapsed nanoseconds to logical frame number
     */
    fun nanosToLogicalFrame(elapsedNanos: Long): Long {
        return elapsedNanos / frameDurationNanos
    }

    /**
     * Convert wall clock timestamp to logical frame
     */
    fun timestampToLogicalFrame(timestamp: Long, sessionStartTimestamp: Long): Long {
        if (sessionStartTimestamp == 0L) {
            throw IllegalStateException("Session not started")
        }

        val relativeTimestamp = timestamp - sessionStartTimestamp
        return if (relativeTimestamp < 0) {
            -1L // Before session start
        } else {
            relativeTimestamp / frameConfig.frameDurationMs
        }
    }

    /**
     * Get current logical frame based on elapsed nanos
     */
    fun getCurrentLogicalFrame(sessionStartNanos: Long): Long {
        if (sessionStartNanos == 0L) return -1L

        val elapsedNanos = System.nanoTime() - sessionStartNanos
        return nanosToLogicalFrame(elapsedNanos)
    }

    /**
     * Calculate when a specific frame should start (in nanos)
     */
    fun getFrameStartNanos(logicalFrame: Long, sessionStartNanos: Long): Long {
        return sessionStartNanos + (logicalFrame * frameDurationNanos)
    }

    /**
     * Calculate nanoseconds until a specific frame starts
     */
    fun nanosUntilFrame(logicalFrame: Long, sessionStartNanos: Long): Long {
        val frameStartNanos = getFrameStartNanos(logicalFrame, sessionStartNanos)
        return frameStartNanos - System.nanoTime()
    }

    /**
     * Calculate milliseconds until a specific frame starts
     */
    fun msUntilFrame(logicalFrame: Long, sessionStartNanos: Long): Long {
        return nanosUntilFrame(logicalFrame, sessionStartNanos) / NANOS_PER_MS
    }

    /**
     * Calculate how many frames behind schedule
     */
    fun calculateFrameLag(currentFrame: Long, sessionStartNanos: Long): Long {
        val expectedFrame = getCurrentLogicalFrame(sessionStartNanos)
        return expectedFrame - currentFrame
    }

    /**
     * Check if lag exceeds threshold (default: 50% of frame duration)
     */
    fun isLaggingBeyondThreshold(
        nanosLate: Long,
        thresholdPercent: Double = 0.5
    ): Boolean {
        val threshold = (frameDurationNanos * thresholdPercent).toLong()
        return nanosLate > threshold
    }

    /**
     * Check if a frame is within allowed buffer limits
     */
    fun isFrameWithinBufferLimit(
        frameNumber: Long,
        frameInProgress: Long,
        maxBufferFrames: Int = frameConfig.maxBufferFrames
    ): Boolean {
        return frameNumber <= frameInProgress + maxBufferFrames
    }

    /**
     * Convert frame duration to readable string
     */
    fun frameDurationToString(): String {
        return when {
            frameConfig.frameDurationMs < 1000 -> "${frameConfig.frameDurationMs}ms"
            else -> "${frameConfig.frameDurationMs / 1000.0}s"
        }
    }

    /**
     * Calculate operations per second based on frame rate
     */
    fun maxOperationsPerSecond(operationsPerFrame: Int): Int {
        val framesPerSecond = 1000.0 / frameConfig.frameDurationMs
        return (operationsPerFrame * framesPerSecond).toInt()
    }
}