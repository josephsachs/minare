package com.minare.core.transport.downsocket.pubsub

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

    private val pendingUpdates = ConcurrentHashMap<String, JsonObject>()
    private val isRunning = AtomicBoolean(false)

    private var batchIntervalMs = 100L // 10 updates per second

    private var timerId: Long? = null

    private var tickCount = 0L
    private var totalProcessingTimeMs = 0L
    private var lastTickTimeMs = 0L

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
    fun queueUpdate(newUpdate: JsonObject) {
        val entityId = newUpdate.getString("_id") ?: run {
            log.warn("Received entity update without _id field: {}", newUpdate.encode())
            return
        }

        pendingUpdates.compute(entityId) { _, currEntry ->
            when {
                currEntry == null -> {
                    log.trace("Added entity to batch queue: id={}", entityId)

                    newUpdate
                }
                newUpdate.getLong("version", 0) > currEntry.getLong("version", 0) -> {
                    currEntry.mergeIn(newUpdate, true)

                    log.trace("Updated entity in batch queue: id={}, version={}",
                        entityId, newUpdate.getLong("version", 0))

                    currEntry
                }
                else -> {
                    log.trace("Ignored outdated entity update: id={}, existing={}, new={}",
                        entityId, currEntry.getLong("version", 0), newUpdate.getLong("version", 0))

                    currEntry
                }
            }
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

        val updatesBatch = HashMap(pendingUpdates)

        pendingUpdates.clear()

        // Create the update message in the same format as DownSocketVerticle currently uses
        // TODO: Send to application hook for developer to convert to client's expected format
        val updateMessage = JsonObject()
            .put("type", "update_batch")
            .put("timestamp", System.currentTimeMillis())
            .put("updates", JsonObject().apply {
                updatesBatch.forEach { (entityId, update) ->
                    put(entityId, update)
                }
            })

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
}