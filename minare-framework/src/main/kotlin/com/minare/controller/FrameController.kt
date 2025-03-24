package com.minare.core

import io.vertx.core.Vertx
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import javax.inject.Inject

/**
 * Base class for frame-based processing controllers.
 * Manages a timing loop that executes processing at regular intervals.
 */
abstract class FrameController @Inject constructor(
    protected val vertx: Vertx
) {
    private val log = LoggerFactory.getLogger(this::class.java)

    // Frame timing and control
    private var frameIntervalMs = 100 // Default: 10 frames per second
    private var frameTimerId: Long? = null
    private val isRunning = AtomicBoolean(false)

    // Performance metrics
    protected var lastFrameTimeMs = 0L
    protected var frameCount = 0L
    protected var totalProcessingTimeMs = 0L

    /**
     * Start the frame loop.
     * @param intervalMs The interval between frames in milliseconds
     */
    fun start(intervalMs: Int = frameIntervalMs) {
        if (isRunning.compareAndSet(false, true)) {
            this.frameIntervalMs = intervalMs
            startFrameLoop()
            log.info("Frame controller started with interval: {}ms", intervalMs)
        } else {
            log.warn("Frame controller already running")
        }
    }

    /**
     * Stop the frame loop.
     */
    fun stop() {
        if (isRunning.compareAndSet(true, false)) {
            frameTimerId?.let { timerId ->
                vertx.cancelTimer(timerId)
                frameTimerId = null
            }
            log.info("Frame controller stopped after {} frames", frameCount)
        } else {
            log.warn("Frame controller not running")
        }
    }

    /**
     * Starts the frame loop timer.
     */
    private fun startFrameLoop() {
        frameTimerId = vertx.setPeriodic(frameIntervalMs.toLong()) { _ ->
            if (!isRunning.get()) {
                return@setPeriodic
            }

            val startTime = System.currentTimeMillis()
            try {
                frameCount++
                tick()
            } catch (e: Exception) {
                log.error("Error in frame processing: {}", e.message, e)
            } finally {
                lastFrameTimeMs = System.currentTimeMillis() - startTime
                totalProcessingTimeMs += lastFrameTimeMs

                // Log warnings if frames are taking too long
                if (lastFrameTimeMs > frameIntervalMs * 0.8) {
                    log.warn("Frame processing took {}ms ({}% of frame budget)",
                        lastFrameTimeMs, (lastFrameTimeMs * 100 / frameIntervalMs))
                }

                // Log periodic stats
                if (frameCount % 100 == 0L) {
                    val avgFrameTime = if (frameCount > 0) totalProcessingTimeMs / frameCount else 0
                    log.info("Frame stats: count={}, avg={}ms, last={}ms",
                        frameCount, avgFrameTime, lastFrameTimeMs)
                }
            }
        }
    }

    /**
     * Process a single frame. This method is called at each frame interval.
     * Subclasses must implement this method to perform their specific frame processing.
     */
    protected abstract fun tick()

    /**
     * Is the frame controller currently running?
     */
    fun isRunning(): Boolean = isRunning.get()

    /**
     * Get the current frame interval in milliseconds.
     */
    fun getFrameIntervalMs(): Int = frameIntervalMs

    /**
     * Set the frame interval in milliseconds.
     * This will take effect on the next frame.
     */
    fun setFrameIntervalMs(intervalMs: Int) {
        if (intervalMs <= 0) {
            throw IllegalArgumentException("Frame interval must be positive")
        }

        if (intervalMs != this.frameIntervalMs) {
            this.frameIntervalMs = intervalMs

            // Restart the timer if we're running
            if (isRunning.get()) {
                frameTimerId?.let { vertx.cancelTimer(it) }
                startFrameLoop()
            }

            log.info("Frame interval updated to {}ms", intervalMs)
        }
    }

    /**
     * Get performance metrics for the frame controller.
     */
    fun getMetrics(): Map<String, Any> {
        val avgFrameTime = if (frameCount > 0) totalProcessingTimeMs / frameCount else 0

        return mapOf(
            "frameCount" to frameCount,
            "averageFrameTimeMs" to avgFrameTime,
            "lastFrameTimeMs" to lastFrameTimeMs,
            "frameIntervalMs" to frameIntervalMs,
            "utilization" to if (frameCount > 0) lastFrameTimeMs.toFloat() / frameIntervalMs else 0f
        )
    }
}