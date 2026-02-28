package com.minare.core

import io.vertx.core.Vertx
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean
import com.google.inject.Inject

/**
 * Base class for timing-based processing.
 * Manages a timing loop that executes processing at regular intervals.
 */
abstract class Timer @Inject constructor(
    protected val vertx: Vertx
) {
    private val log = LoggerFactory.getLogger(this::class.java)


    private var intervalMs = 20 // 10 ticks per second
    private var timerId: Long? = null
    private val isRunning = AtomicBoolean(false)

    protected var lastTickTimeMs = 0L
    protected var counter = 0L
    protected var totalProcessingTimeMs = 0L

    /**
     * Start the loop.
     * @param intervalMs The interval between ticks in milliseconds
     */
    fun start(intervalMs: Int = this.intervalMs) {
        if (isRunning.compareAndSet(false, true)) {
            this.intervalMs = intervalMs
            startTimer()
            log.info("Timer started with interval: {}ms", intervalMs)
        } else {
            log.warn("Timer already running")
        }
    }

    /**
     * Stop the loop.
     */
    fun stop() {
        if (isRunning.compareAndSet(true, false)) {
            timerId?.let { id ->
                vertx.cancelTimer(id)
                timerId = null
            }
            log.info("Timer stopped after {} ticks", counter)
        } else {
            log.warn("Timer not running")
        }
    }

    /**
     * Starts the loop timer.
     */
    private fun startTimer() {
        timerId = vertx.setPeriodic(intervalMs.toLong()) { _ ->
            if (!isRunning.get()) {
                return@setPeriodic
            }

            val startTime = System.currentTimeMillis()
            try {
                counter++
                tick()
            } catch (e: Exception) {
                log.error("Error in tick processing: {}", e.message, e)
            } finally {
                lastTickTimeMs = System.currentTimeMillis() - startTime
                totalProcessingTimeMs += lastTickTimeMs
            }
        }
    }

    /**
     * Process a single tick. This method is called at each tick interval.
     * Subclasses must implement this method to perform their specific tick processing.
     */
    protected abstract fun tick()

    /**
     * Is the timer currently running?
     */
    fun isRunning(): Boolean = isRunning.get()

    /**
     * Get the current tick interval in milliseconds.
     */
    fun getIntervalMs(): Int = intervalMs

    /**
     * Set the tick interval in milliseconds.
     * This will take effect on the next tick.
     */
    fun setIntervalMs(intervalMs: Int) {
        if (intervalMs <= 0) {
            throw IllegalArgumentException("Tick interval must be positive")
        }

        if (intervalMs != this.intervalMs) {
            this.intervalMs = intervalMs

            if (isRunning.get()) {
                timerId?.let { vertx.cancelTimer(it) }
                startTimer()
            }

            log.info("Tick interval updated to {}ms", intervalMs)
        }
    }

    /**
     * Get performance metrics for the timer.
     */
    fun getMetrics(): Map<String, Any> {
        val avgTickTime = if (counter > 0) totalProcessingTimeMs / counter else 0

        return mapOf(
            "counter" to counter,
            "averageTickTimeMs" to avgTickTime,
            "lastTickTimeMs" to lastTickTimeMs,
            "intervalMs" to intervalMs,
            "utilization" to if (counter > 0) lastTickTimeMs.toFloat() / intervalMs else 0f
        )
    }
}