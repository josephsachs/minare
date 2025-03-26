package com.minare.worker.command.events

import com.google.inject.Inject
import com.minare.utils.EventBusUtils
import com.minare.utils.HttpServerUtils
import com.minare.utils.VerticleLogger
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory
import com.minare.worker.command.CommandVerticle
import com.minare.worker.command.CommandVerticle.Companion.HTTP_SERVER_HOST
import com.minare.worker.command.CommandVerticle.Companion.HTTP_SERVER_PORT

class CommandSocketInitEvent @Inject constructor(
    private val eventBusUtils: EventBusUtils,
    private val vlog: VerticleLogger
) {
    public suspend fun register(context: CommandVerticle) {
        eventBusUtils.registerTracedConsumer<JsonObject>(ADDRESS_COMMAND_SOCKET_INITIALIZE) { message, traceId ->
            try {
                vlog.logStartupStep("INITIALIZING_ROUTER", mapOf("traceId" to traceId))

                val startTime = System.currentTimeMillis()
                // Router is already initialized in start() method
                val initTime = System.currentTimeMillis() - startTime

                vlog.logVerticlePerformance("ROUTER_INITIALIZATION", initTime)
                context.deployOwnHttpServer()

                val reply = JsonObject()
                    .put("success", true)
                    .put("message", "Command socket router initialized with dedicated HTTP server on port $HTTP_SERVER_HOST")

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
        const val ADDRESS_COMMAND_SOCKET_INITIALIZE = "minare.command.socket.initialize"
    }
}