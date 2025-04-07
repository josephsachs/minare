package com.minare.utils

import io.vertx.core.AsyncResult
import io.vertx.core.Future
import io.vertx.core.Handler
import io.vertx.core.Vertx
import io.vertx.core.eventbus.DeliveryOptions
import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

/**
 * Utility class for event bus communications with tracing support
 * Provides methods to send and receive traced messages across verticles
 */
class EventBusUtils(
    private val vertx: Vertx,
    private val coroutineContext: CoroutineContext,
    private val component: String
) {
    private val log = LoggerFactory.getLogger(EventBusUtils::class.java)
    private val eventLog = EventLogger.forComponent(component)

    /**
     * Send a traced message to the event bus and await a reply
     */
    suspend fun <T> sendWithTracing(
        address: String,
        message: Any,
        parentTraceId: String? = null
    ): T {
        val traceId = parentTraceId ?: eventLog.trace("EVENTBUS_SEND",
            mapOf("address" to address, "component" to component))

        eventLog.logSend(address, message, traceId)

        val options = DeliveryOptions().addHeader("traceId", traceId)

        try {
            val reply = vertx.eventBus().request<T>(address, message, options).await()

            eventLog.trace("EVENTBUS_REPLY_RECEIVED",
                mapOf("address" to address, "status" to "success"), traceId)

            if (parentTraceId == null) {
                // Only end trace if we started it
                eventLog.endTrace(traceId, "EVENTBUS_COMPLETE",
                    mapOf("address" to address, "status" to "success"))
            }

            return reply.body()
        } catch (e: Exception) {
            eventLog.logError("EVENTBUS_SEND_FAILED", e,
                mapOf("address" to address), traceId)

            if (parentTraceId == null) {
                eventLog.endTrace(traceId, "EVENTBUS_COMPLETE",
                    mapOf("address" to address, "status" to "failed", "error" to e.message))
            }

            throw e
        }
    }

    /**
     * Helper function to extract trace ID from message headers
     */
    fun extractTraceId(message: Message<*>): String? {
        return message.headers().get("traceId")
    }

    /**
     * Register a consumer with tracing support
     */
    fun <T> registerTracedConsumer(
        address: String,
        handler: suspend (Message<T>, String) -> Unit
    ) {
        vertx.eventBus().consumer<T>(address) { message ->
            val traceId = extractTraceId(message) ?: eventLog.logReceive(message)

            CoroutineScope(coroutineContext).launch {
                try {
                    handler(message, traceId)
                } catch (e: Exception) {
                    eventLog.logError("CONSUMER_HANDLER_FAILED", e,
                        mapOf("address" to address), traceId)

                    // Send error back to caller
                    message.fail(500, e.message ?: "Error processing message")
                }
            }
        }
    }

    /**
     * Create a traced reply to a message
     */
    fun <T, R> tracedReply(message: Message<T>, reply: R, traceId: String) {
        eventLog.logReply(message, reply as Any, traceId)
        message.reply(reply)
    }

    /**
     * Create a traced error reply to a message
     */
    fun <T> tracedFail(message: Message<T>, code: Int, errorMessage: String, traceId: String) {
        eventLog.trace("EVENTBUS_REPLY_ERROR",
            mapOf("code" to code, "error" to errorMessage), traceId)
        message.fail(code, errorMessage)
    }

    companion object {
        /**
         * Add trace ID to a message for manual tracing
         */
        fun addTraceId(message: JsonObject, traceId: String): JsonObject {
            return message.copy().put("_traceId", traceId)
        }

        /**
         * Extract trace ID from a message for manual tracing
         */
        fun getTraceId(message: JsonObject): String? {
            return message.getString("_traceId")
        }
    }
}