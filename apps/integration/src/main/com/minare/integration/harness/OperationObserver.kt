package com.minare.integration.harness

import com.hazelcast.core.HazelcastInstance
import com.minare.worker.coordinator.models.OperationCompletion
import io.vertx.core.Vertx
import io.vertx.core.eventbus.MessageConsumer
import io.vertx.core.json.JsonObject
import java.util.concurrent.CopyOnWriteArrayList

/**
 * Subscribes to frame completion events on the cluster event bus and queries
 * the operation-completions Hazelcast map to provide server-side diagnostics
 * when an operation test times out.
 *
 * Lifecycle: call start() before the operation under test; stop() in a finally block.
 */
class OperationObserver(
    private val vertx: Vertx,
    private val hazelcast: HazelcastInstance
) {
    data class FrameEvent(
        val logicalFrame: Long,
        val workerId: String,
        val operationCount: Int,
        val receivedAt: Long = System.currentTimeMillis()
    )

    private val frameEvents = CopyOnWriteArrayList<FrameEvent>()
    private var consumer: MessageConsumer<JsonObject>? = null

    fun start() {
        consumer = vertx.eventBus().consumer("minare.coordinator.worker.frame.complete") { msg ->
            val body = msg.body()
            frameEvents += FrameEvent(
                logicalFrame = body.getLong("logicalFrame") ?: -1L,
                workerId = body.getString("workerId") ?: "unknown",
                operationCount = body.getInteger("operationCount", 0)
            )
        }
    }

    fun stop() {
        consumer?.unregister()
    }

    /** Look up a specific operation in the distributed completion map. */
    fun queryCompletion(operationId: String): OperationCompletion? {
        val map = hazelcast.getMap<String, OperationCompletion>("operation-completions")
        return map.entries.find { it.key.contains(":op-$operationId") }?.value
    }

    /**
     * Append a server-side state snapshot to the step log.
     * Call this in a timeout catch block before rethrowing.
     */
    fun diagnose(operationId: String?, log: TestStepLog) {
        val framesWithOps = frameEvents.filter { it.operationCount > 0 }
        val completion = operationId?.let { queryCompletion(it) }

        log.step(
            "server-side state at failure",
            "framesObserved" to frameEvents.size,
            "framesWithOperations" to framesWithOps.size,
            "operationId" to (operationId ?: "—"),
            "operationFoundInCompletionMap" to (completion != null),
            "completionWorker" to (completion?.workerId ?: "—"),
            "completionEntityId" to (completion?.entityId ?: "—"),
            "completionSummary" to (completion?.resultSummary ?: "—")
        )

        if (framesWithOps.isNotEmpty()) {
            log.step(
                "frames that processed operations",
                *framesWithOps.takeLast(5).mapIndexed { i, f ->
                    "frame[${framesWithOps.size - framesWithOps.takeLast(5).size + i}]" to
                            "logicalFrame=${f.logicalFrame} worker=${f.workerId} ops=${f.operationCount}"
                }.toTypedArray()
            )
        }
    }
}
