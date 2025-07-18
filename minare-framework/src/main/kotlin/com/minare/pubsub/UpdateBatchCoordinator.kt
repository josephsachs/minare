package com.minare.pubsub

import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Coordinates batching of entity updates across all DownSocketVerticles.
 *
 * This class collects entity updates from Redis and distributes them in batches
 * at regular intervals to ensure all DownSocketVerticles receive identical update batches.
 */
@Singleton
class UpdateBatchCoordinator @Inject constructor(
    private val vertx: Vertx
) {
    private val log = LoggerFactory.getLogger(UpdateBatchCoordinator::class.java)

    // Map of entityId -> latest update
    private val pendingUpdates = ConcurrentHashMap<String, JsonObject>()

    // Whether the coordinator is running
    private val isRunning = AtomicBoolean(false)

    // The interval between batch distributions (milliseconds)
    private var batchIntervalMs = 100L // 10 updates per second

    // Timer ID for the periodic batch distribution
    private var timerId: Long? = null

    // Metrics
    private var tickCount = 0L
    private var totalProcessingTimeMs = 0L
    private var lastTickTimeMs = 0L

    // Address to publish batched updates
    companion object {
        const val ADDRESS_BATCHED_UPDATES = "minare.entity.batched.updates"
    }

    /**
     * Start the batch coordinator
     */
    fun start(intervalMs: Long = batchIntervalMs) {
        if (isRunning.compareAndSet(false, true)) {
            this.batchIntervalMs = intervalMs
            startBatchTimer()
            log.info("Started UpdateBatchCoordinator with batch interval {}ms", batchIntervalMs)
        } else {
            log.warn("UpdateBatchCoordinator already running")
        }
    }

    /**
     * Stop the batch coordinator
     */
    fun stop() {
        if (isRunning.compareAndSet(true, false)) {
            timerId?.let { vertx.cancelTimer(it) }
            timerId = null
            log.info("UpdateBatchCoordinator stopped after {} ticks", tickCount)
        } else {
            log.warn("UpdateBatchCoordinator not running")
        }
    }

    /**
     * Set the batch processing interval
     */
    fun setBatchInterval(intervalMs: Long) {
        if (intervalMs <= 0) {
            throw IllegalArgumentException("Batch interval must be positive")
        }

        if (intervalMs != this.batchIntervalMs) {
            this.batchIntervalMs = intervalMs

            if (isRunning.get()) {
                // Restart the timer with the new interval
                timerId?.let { vertx.cancelTimer(it) }
                startBatchTimer()
            }

            log.info("Batch interval updated to {}ms", intervalMs)
        }
    }

    /**
     * Queue an entity update for processing in the next batch.
     * If an update for the same entity already exists, it will only be replaced
     * if the new update has a higher version number.
     */
    fun queueUpdate(entityUpdate: JsonObject) {
        val entityId = entityUpdate.getString("_id")
        if (entityId == null) {
            log.warn("Received entity update without _id field: {}", entityUpdate.encode())
            return
        }

        // Check if we already have an update for this entity
        val existingUpdate = pendingUpdates[entityId]

        if (existingUpdate != null) {
            // Compare versions and only replace if newer
            val existingVersion = existingUpdate.getLong("version", 0)
            val newVersion = entityUpdate.getLong("version", 0)

            if (newVersion > existingVersion) {
                // New version is higher, replace the pending update
                pendingUpdates[entityId] = entityUpdate
                log.trace("Updated entity in batch queue: id={}, version={}", entityId, newVersion)
            } else {
                log.trace("Ignored outdated entity update: id={}, existing={}, new={}",
                    entityId, existingVersion, newVersion)
            }
        } else {
            // No existing update, add this one
            pendingUpdates[entityId] = entityUpdate
            log.trace("Added entity to batch queue: id={}", entityId)
        }
    }

    /**
     * Start the timer for batch distribution
     */
    private fun startBatchTimer() {
        timerId = vertx.setPeriodic(batchIntervalMs) { _ ->
            if (!isRunning.get()) {
                return@setPeriodic
            }

            val startTime = System.currentTimeMillis()
            try {
                tickCount++
                distributeBatch()
            } catch (e: Exception) {
                log.error("Error in batch distribution: {}", e.message, e)
            } finally {
                lastTickTimeMs = System.currentTimeMillis() - startTime
                totalProcessingTimeMs += lastTickTimeMs

                if (lastTickTimeMs > batchIntervalMs * 0.8) {
                    log.warn("Batch processing took {}ms ({}% of batch interval)",
                        lastTickTimeMs, (lastTickTimeMs * 100 / batchIntervalMs))
                }

                if (tickCount % 100 == 0L) {
                    val avgTickTime = if (tickCount > 0) totalProcessingTimeMs / tickCount else 0
                    log.info("Batch stats: count={}, avg={}ms, last={}ms",
                        tickCount, avgTickTime, lastTickTimeMs)
                }
            }
        }
    }

    /**
     * Distribute the current batch of updates to all DownSocketVerticles
     */
    private fun distributeBatch() {
        if (pendingUpdates.isEmpty()) {
            return
        }

        // Create a snapshot of current updates and clear pending queue
        val updatesBatch = HashMap(pendingUpdates)
        pendingUpdates.clear()

        // Create the update message in the same format as DownSocketVerticle currently uses
        val updateMessage = JsonObject()
            .put("type", "update_batch")
            .put("timestamp", System.currentTimeMillis())
            .put("updates", JsonObject().apply {
                updatesBatch.forEach { (entityId, update) ->
                    put(entityId, update)
                }
            })

        // Publish to event bus for all DownSocketVerticles to consume
        vertx.eventBus().publish(ADDRESS_BATCHED_UPDATES, updateMessage)

        if (updatesBatch.size > 0 && (
                    updatesBatch.size > 100 ||
                            lastTickTimeMs > batchIntervalMs / 2 ||
                            tickCount % 100 == 0L
                    )) {
            log.info("Distributed batch with {} entity updates in {}ms",
                updatesBatch.size, lastTickTimeMs)
        }
    }

    /**
     * Get performance metrics for the batch coordinator
     */
    fun getMetrics(): Map<String, Any> {
        val avgTickTime = if (tickCount > 0) totalProcessingTimeMs / tickCount else 0

        return mapOf(
            "counter" to tickCount,
            "averageTickTimeMs" to avgTickTime,
            "lastTickTimeMs" to lastTickTimeMs,
            "batchIntervalMs" to batchIntervalMs,
            "utilization" to if (tickCount > 0) lastTickTimeMs.toFloat() / batchIntervalMs else 0f,
            "pendingUpdatesCount" to pendingUpdates.size
        )
    }
}