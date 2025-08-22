package com.minare.worker.coordinator

import com.minare.time.FrameCalculator
import com.minare.time.FrameConfiguration
import io.vertx.core.Vertx
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Handles all frame scheduling logic for the coordinator.
 * Responsible for determining when frames should be prepared and
 * managing the timing of frame preparation.
 *
 * Governed by lookahead buffer and coordinator manifest pause state.
 */
@Singleton
class FrameScheduler @Inject constructor(
    private val frameCalculator: FrameCalculator,
    private val frameConfig: FrameConfiguration,
    private val coordinatorState: FrameCoordinatorState,
    private val vertx: Vertx
) {
    private val log = LoggerFactory.getLogger(FrameScheduler::class.java)

    /**
     * Schedule preparation of a specific frame at its start time.
     * This is the main entry point for frame scheduling.
     */
    fun scheduleFramePreparation(
        frameNumber: Long,
        scope: CoroutineScope,
        onPrepareFrame: suspend (Long) -> Unit
    ) {
        // Skip if already prepared
        if (frameNumber <= coordinatorState.lastPreparedManifest) {
            val nextUnpreparedFrame = coordinatorState.lastPreparedManifest + 1
            scheduleFramePreparation(nextUnpreparedFrame, scope, onPrepareFrame)
            return
        }

        val timeLeft = frameCalculator.msUntilFrame(frameNumber, coordinatorState.sessionStartNanos)

        if (timeLeft > 0) {
            scheduleAtExactTime(frameNumber, timeLeft, scope, onPrepareFrame)
        } else {
            scope.launch {
                onPrepareFrame(frameNumber)
                scheduleNextFrame(scope, onPrepareFrame)
            }
        }
    }

    /**
     * Get frames that should be prepared right now based on current state
     */
    fun getFramesToPrepareNow(): List<Long> {
        val lastPrepared = coordinatorState.lastPreparedManifest

        return getFramesToPrepareNormally(lastPrepared)
    }

    /**
     * Check if a specific frame should be prepared given current constraints
     */
    fun shouldPrepareFrame(frameNumber: Long): Boolean {
        // Already prepared?
        if (frameNumber <= coordinatorState.lastPreparedManifest) {
            return false
        }

        // Normal operation - prepare if within lookahead
        val currentFrame = frameCalculator.getCurrentLogicalFrame(coordinatorState.sessionStartNanos)
        return frameNumber <= currentFrame + frameConfig.normalOperationLookahead
    }

    private fun scheduleAtExactTime(
        frameNumber: Long,
        delayMs: Long,
        scope: CoroutineScope,
        onPrepareFrame: suspend (Long) -> Unit
    ) {
        vertx.setTimer(delayMs) {
            scope.launch {
                handleScheduledFrame(frameNumber, scope, onPrepareFrame)
            }
        }
    }

    private suspend fun handleScheduledFrame(
        frameNumber: Long,
        scope: CoroutineScope,
        onPrepareFrame: suspend (Long) -> Unit
    ) {
        // Only prepare if still needed (operation arrival might have triggered it)
        if (!shouldPrepareFrame(frameNumber)) {
            log.debug("Frame {} already prepared or outside allowed range", frameNumber)
            return
        }

        onPrepareFrame(frameNumber)
        scheduleFramePreparation(frameNumber + 1, scope, onPrepareFrame)
    }

    private fun scheduleNextFrame(
        scope: CoroutineScope,
        onPrepareFrame: suspend (Long) -> Unit
    ) {
        val currentFrame = frameCalculator.getCurrentLogicalFrame(coordinatorState.sessionStartNanos)
        val nextFrame = maxOf(
            currentFrame + 1,
            coordinatorState.lastPreparedManifest + 1
        )

        scheduleFramePreparation(nextFrame, scope, onPrepareFrame)
    }

    private fun getFramesToPrepareNormally(lastPrepared: Long): List<Long> {
        val currentFrame = frameCalculator.getCurrentLogicalFrame(coordinatorState.sessionStartNanos)
        val targetFrame = currentFrame + frameConfig.normalOperationLookahead

        return if (targetFrame > lastPrepared) {
            ((lastPrepared + 1)..targetFrame).toList()
        } else {
            emptyList()
        }
    }
}