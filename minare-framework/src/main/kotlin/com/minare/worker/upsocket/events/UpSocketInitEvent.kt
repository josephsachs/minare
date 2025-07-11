package com.minare.worker.upsocket.events

import com.google.inject.Inject
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import com.minare.worker.upsocket.UpSocketVerticle.Companion.HTTP_SERVER_HOST

class UpSocketInitEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger
) {
    public suspend fun register() {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_UP_SOCKET_INITIALIZE) { message, traceId ->
            try {
                vlog.logStartupStep("INITIALIZING_ROUTER", mapOf("traceId" to traceId))

                val startTime = System.currentTimeMillis()
                // Router is already initialized in start() method
                val initTime = System.currentTimeMillis() - startTime

                vlog.logVerticlePerformance("ROUTER_INITIALIZATION", initTime)

                val reply = JsonObject()
                    .put("success", true)
                    .put("message", "Up socket router initialized with dedicated HTTP server on port $HTTP_SERVER_HOST")

                eventBusUtils.tracedReply(message, reply, traceId)

                vlog.logStartupStep(
                    "ROUTER_INITIALIZED", mapOf(
                        "status" to "success",
                        "initTime" to initTime,
                        "useOwnHttpServer" to true
                    )
                )
            } catch (e: Exception) {
                vlog.logVerticleError("INITIALIZE_ROUTER", e)
                message.fail(500, "Failed to initialize router: ${e.message}")
            }
        }
    }

    companion object {
        const val ADDRESS_UP_SOCKET_INITIALIZE = "minare.up.socket.initialize"
    }
}