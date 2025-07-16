package kotlin.com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.coordinator.WorkerRegistry
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import com.minare.worker.coordinator.FrameCoordinatorVerticle.FrameCompletion
import io.vertx.core.json.JsonObject

class WorkerFrameCompleteEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry
) {
    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_FRAME_COMPLETE) { message, traceId ->
            val workerId = message.body().getString("workerId")
            val frameStartTime = message.body().getLong("frameStartTime")
            val operationCount = message.body().getInteger("operationCount", 0)

            frameCompletions[workerId] = FrameCompletion(
                workerId = workerId,
                frameStartTime = frameStartTime,
                operationCount = operationCount,
                completedAt = System.currentTimeMillis()
            )

            // Track in worker registry
            workerRegistry.recordFrameCompletion(workerId, frameStartTime)

            vlog.logInfo("Worker $workerId completed frame $frameStartTime with $operationCount operations")
        }
    }

    companion object {
        const val ADDRESS_WORKER_FRAME_COMPLETE = "minare.coordinator.worker.frame.complete"
    }
}