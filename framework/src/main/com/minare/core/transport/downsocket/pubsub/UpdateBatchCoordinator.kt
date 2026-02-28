package com.minare.core.transport.downsocket.pubsub

import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.storage.interfaces.ContextStore
import com.minare.core.transport.downsocket.DownSocketVerticle
import com.minare.core.utils.debug.DebugLogger
import com.minare.core.utils.debug.DebugLogger.Companion.DebugType
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean
import com.google.inject.Inject
import com.google.inject.Singleton
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import io.vertx.kotlin.coroutines.dispatcher

@Singleton
class UpdateBatchCoordinator @Inject constructor(
    private val vertx: Vertx,
    private val connectionStore: ConnectionStore,
    private val contextStore: ContextStore,
    private val channelStore: ChannelStore,
    private val debug: DebugLogger
) {
    private val log = LoggerFactory.getLogger(UpdateBatchCoordinator::class.java)

    private val pendingUpdates = ConcurrentHashMap<String, JsonObject>()
    private val isRunning = AtomicBoolean(false)

    private var batchIntervalMs = 0L
    private var timerId: Long? = null

    companion object {
        const val ADDRESS_BATCHED_UPDATES = "minare.entity.batched.updates"
    }

    fun start(intervalMs: Long) {
        if (!isRunning.compareAndSet(false, true)) {
            log.warn("UpdateBatchCoordinator already running")
            return
        }

        batchIntervalMs = intervalMs

        if (shouldUseBatchTimer()) {
            startBatchTimer()
            debug.log(DebugType.DOWNSOCKET_PUBSUB_STARTED_WITH_BATCHING, listOf(batchIntervalMs))
        } else {
            debug.log(DebugType.DOWNSOCKET_PUBSUB_STARTED_NO_BATCHING, listOf(batchIntervalMs))
        }
    }

    fun stop() {
        if (!isRunning.compareAndSet(true, false)) {
            return
        }

        stopBatchTimer()
    }

    fun queueUpdate(entityUpdate: JsonObject) {
        val entityId = extractEntityId(entityUpdate) ?:
        run {
            log.error("Received change update for entity missing ID: ${entityUpdate.encodePrettily()}")
            return
        }

        val existingUpdate = pendingUpdates[entityId]
        if (existingUpdate == null) {
            pendingUpdates[entityId] = entityUpdate
            return
        }

        if (isNewerVersion(entityUpdate, existingUpdate)) {
            pendingUpdates[entityId] = existingUpdate.mergeIn(entityUpdate, true)
        }
    }

    fun flushBatch() {
        distributeBatch()
    }

    private fun shouldUseBatchTimer(): Boolean {
        return batchIntervalMs > 0
    }

    private fun extractEntityId(entityUpdate: JsonObject): String? {
        return entityUpdate.getString("_id")
    }

    private fun getVersion(entityUpdate: JsonObject): Long {
        return entityUpdate.getLong("version", 0)
    }

    private fun isNewerVersion(newUpdate: JsonObject, existingUpdate: JsonObject): Boolean {
        return getVersion(newUpdate) > getVersion(existingUpdate)
    }

    private fun startBatchTimer() {
        timerId = vertx.setPeriodic(batchIntervalMs) {
            if (!isRunning.get()) {
                return@setPeriodic
            }

            try {
                distributeBatch()
            } catch (e: Exception) {
                log.error("Error in batch distribution: {}", e.message, e)
            }
        }
    }

    private fun stopBatchTimer() {
        timerId?.let { vertx.cancelTimer(it) }
        timerId = null
    }

    /**
     * Fan out each pending entity update to the specific DownSocketVerticle instance
     * that owns each subscribed connection, using downSocketDeploymentId for targeting.
     */
    private fun distributeBatch() {
        if (pendingUpdates.isEmpty()) {
            return
        }

        val updatesBatch = HashMap(pendingUpdates)
        pendingUpdates.clear()

        CoroutineScope(vertx.dispatcher()).launch {
            try {
                routeUpdatesToConnections(updatesBatch)
            } catch (e: Exception) {
                log.error("Error routing batch updates to connections", e)
            }
        }

        debug.log(DebugType.DOWNSOCKET_PUBSUB_DISTRIBUTED_BATCH, listOf(updatesBatch.size))
    }

    private suspend fun routeUpdatesToConnections(updatesBatch: Map<String, JsonObject>) {
        for ((entityId, entityUpdate) in updatesBatch) {
            val channels = contextStore.getChannelsByEntityId(entityId)
            if (channels.isEmpty()) continue

            val processedConnections = mutableSetOf<String>()

            for (channelId in channels) {
                val connectionIds = channelStore.getClientIds(channelId)
                val connections = connectionStore.find(connectionIds.toSet())

                for (connection in connections) {
                    if (connection.id in processedConnections) continue
                    processedConnections.add(connection.id)

                    val downInstanceId = connection.downSocketDeploymentId ?: continue

                    vertx.eventBus().send(
                        "${DownSocketVerticle.ADDRESS_SEND_TO_DOWN_CONNECTION}.${downInstanceId}",
                        JsonObject()
                            .put("connectionId", connection.id)
                            .put("entityId", entityId)
                            .put("update", entityUpdate)
                    )
                }
            }
        }
    }
}