package com.minare.core.frames.coordinator.services

import com.minare.application.config.FrameworkConfig
import com.google.inject.Inject
import com.google.inject.Singleton
import kotlin.math.max

/**
 * Injectable utility for frame-related calculations.
 * Encapsulates frame math logic and the FrameConfiguration dependency.
 */
@Singleton
class FrameCalculatorService @Inject constructor(
    val frameworkConfig: FrameworkConfig
) {
    companion object {
        const val NANOS_PER_MS = 1_000_000L
        const val NANOS_PER_SECOND = 1_000_000_000L
    }

    private val frameDurationNanos = frameworkConfig.frames.frameDuration * NANOS_PER_MS

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
            // No session yet - treat as very late for frame 0
            return 0L
        }

        val relativeTimestamp = timestamp - sessionStartTimestamp
        return relativeTimestamp / frameworkConfig.frames.frameDuration
    }

    /**
     * Get current logical frame based on elapsed nanos
     */
    fun getCurrentLogicalFrame(sessionStartNanos: Long): Long {
        if (sessionStartNanos == 0L) return -1L

        val elapsedNanos = System.nanoTime() - sessionStartNanos

        return nanosToLogicalFrame(elapsedNanos)
    }

    fun msUntilFrameEnd(currentFrame: Long, sessionStartNanos: Long): Long {
        val nextFrameStart = getFrameStartNanos(currentFrame + 1, sessionStartNanos)
        val delayNanos = nextFrameStart - System.nanoTime()
        return max(0, nanosToMs(delayNanos))
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
        return nanosToMs(nanosUntilFrame(logicalFrame, sessionStartNanos))
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
     * Convert frame duration to readable string
     */
    fun frameDurationToString(): String {
        val frameDuration = frameworkConfig.frames.frameDuration

        return when {
            frameDuration < 1000 -> "${frameDuration}ms"
            else -> "${frameDuration / 1000.0}s"
        }
    }

    /**
     * Calculate operations per second based on frame rate
     */
    fun maxOperationsPerSecond(operationsPerFrame: Int): Int {
        val framesPerSecond = 1000.0 / frameworkConfig.frames.frameDuration
        return (operationsPerFrame * framesPerSecond).toInt()
    }

    /**
     * Convert nanoseconds to milliseconds
     */
    fun nanosToMs(nanos: Long): Long {
        return nanos / NANOS_PER_MS
    }

    /**
     * Convert nanoseconds to seconds (with decimal precision)
     */
    fun nanosToSeconds(nanos: Long): Double {
        return nanos.toDouble() / NANOS_PER_SECOND
    }

    /**
     * Convert milliseconds to nanoseconds
     */
    fun msToNanos(ms: Long): Long {
        return ms * NANOS_PER_MS
    }

    /**
     * Determine the severity of frame lag
     * In our real-time system, any lag is concerning
     * @param framesBehind Number of frames behind expected (negative means ahead)
     * @return Severity level of the lag
     */
    fun getFrameLagSeverity(framesBehind: Long): LagSeverity {
        return when {
            framesBehind < 0 -> LagSeverity.INVALID  // Should never be ahead
            framesBehind == 0L -> LagSeverity.NONE
            framesBehind == 1L -> LagSeverity.WARNING
            else -> LagSeverity.CRITICAL
        }
    }

    /**
     * Check if frame processing is healthy based on lag
     * @param framesBehind Number of frames behind expected
     * @return true if exactly on schedule (0 frames behind)
     */
    fun isFrameProcessingHealthy(framesBehind: Long): Boolean {
        return framesBehind == 0L
    }

    /**
     * Get comprehensive frame processing status
     * @param currentFrame Frame currently being processed
     * @param expectedFrame Frame that should be processed based on time
     * @return Detailed status of frame processing
     */
    fun getFrameProcessingStatus(currentFrame: Long, expectedFrame: Long): FrameProcessingStatus {
        val framesBehind = expectedFrame - currentFrame
        val severity = getFrameLagSeverity(framesBehind)
        val isHealthy = isFrameProcessingHealthy(framesBehind)

        return FrameProcessingStatus(
            currentFrame = currentFrame,
            expectedFrame = expectedFrame,
            framesBehind = framesBehind,
            lagSeverity = severity,
            isHealthy = isHealthy,
            recommendedAction = when (severity) {
                LagSeverity.NONE -> "Normal operation"
                LagSeverity.WARNING -> "Frame processing delayed - investigate immediately"
                LagSeverity.CRITICAL -> "Critical lag detected - system recovery needed"
                LagSeverity.INVALID -> "Invalid state: processing ahead of schedule"
            }
        )
    }

    /**
     * Lag severity levels for frame processing
     * In our real-time system, we have strict requirements
     */
    enum class LagSeverity {
        NONE,      // Exactly on schedule (0 frames behind)
        WARNING,   // FRAME_LAG_WARNINGL_THRESHOLD frames behind - immediate attention needed
        CRITICAL,  // More than FRAME_LAG_CRITICAL_THRESHOLD frames behind - system recovery required
        INVALID    // Ahead of schedule - indicates logical error as coordinator should prevent this
    }

    /**
     * Comprehensive frame processing status
     */
    data class FrameProcessingStatus(
        val currentFrame: Long,
        val expectedFrame: Long,
        val framesBehind: Long,
        val lagSeverity: LagSeverity,
        val isHealthy: Boolean,
        val recommendedAction: String
    )
}