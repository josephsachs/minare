package com.minare.worker.coordinator.events

import com.google.inject.Inject
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject

/**
 * Event handler for next frame processing signals.
 * Workers listen to this event to know when to process the next frame.
 *
 * Note: This event handler is primarily for logging/tracing purposes.
 * The actual handling logic is implemented directly in FrameWorkerVerticle.
 */
class NextFrameEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger
) {
    /**suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_NEXT_FRAME) { message, traceId ->
            vlog.getEventLogger().trace(
                "NEXT_FRAME_EVENT",
                mapOf(
                    "timestamp" to System.currentTimeMillis()
                ),
                traceId
            )
            // Workers handle this event in FrameWorkerVerticle.handleNextFrame()
        }
    }**/

    companion object {
        const val ADDRESS_NEXT_FRAME = "minare.coordinator.next.frame"
    }
}