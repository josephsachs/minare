package kotlin.com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.coordinator.WorkerRegistry
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject

class WorkerRegisterEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry
) {
    suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_WORKER_REGISTER) { message, traceId ->
            val workerId = message.body().getString("workerId")

            val registered = workerRegistry.registerWorker(workerId)

            if (registered) {
                    // If we were waiting for workers, check if we can start now
                if (!isPaused && workerRegistry.hasMinimumWorkers()) {
                    ensureFrameLoopRunning()
                }
            }
        }
    }

    companion object {
        const val ADDRESS_WORKER_REGISTER = "minare.coordinator.worker.register"
    }
}