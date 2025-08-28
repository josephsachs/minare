package com.minare.utils

import io.vertx.core.eventbus.Message
import io.vertx.core.json.JsonObject
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.util.UUID

/**
 * Utility for structured logging of events and message flows in a reactive architecture.
 * Makes it easier to trace message chains across verticles.
 */
class EventLogger(private val component: String) {
    private val log: Logger = LoggerFactory.getLogger("com.minare.event.$component")


    private val traces = mutableMapOf<String, Long>()

    /**
     * Create or continue a trace for related events
     */
    fun trace(action: String, details: Map<String, Any?> = emptyMap(), traceId: String? = null): String {
        val actualTraceId = traceId ?: UUID.randomUUID().toString()
        val timestamp = System.currentTimeMillis()


        traces[actualTraceId] = timestamp

        val logEntry = JsonObject()
            .put("component", component)
            .put("action", action)
            .put("traceId", actualTraceId)
            .put("timestamp", timestamp)

        details.forEach { (key, value) ->
            if (value != null) {
                logEntry.put(key, value.toString())
            }
        }

        log.info("[TRACE] {}", logEntry.encode())
        return actualTraceId
    }

    /**
     * End a trace and log the duration
     */
    fun endTrace(traceId: String, action: String, details: Map<String, Any?> = emptyMap()): Long {
        val timestamp = System.currentTimeMillis()
        val startTime = traces.remove(traceId) ?: timestamp
        val duration = timestamp - startTime

        val logEntry = JsonObject()
            .put("component", component)
            .put("action", action)
            .put("traceId", traceId)
            .put("timestamp", timestamp)
            .put("duration_ms", duration)
            .put("complete", true)

        details.forEach { (key, value) ->
            if (value != null) {
                logEntry.put(key, value.toString())
            }
        }

        log.info("[TRACE-END] {}", logEntry.encode())
        return duration
    }

    /**
     * Log an event bus message being sent
     */
    fun logSend(address: String, message: Any, traceId: String? = null): String {
        val actualTraceId = traceId ?: UUID.randomUUID().toString()

        val details = mutableMapOf<String, Any?>()
        details["address"] = address
        details["direction"] = "OUT"

        if (message is JsonObject) {

            details["messageFields"] = message.fieldNames().joinToString(",")
        }

        return trace("MESSAGE_SEND", details, actualTraceId)
    }

    /**
     * Log an event bus message being received
     */
    fun <T> logReceive(message: Message<T>, action: String = "MESSAGE_RECEIVE", traceId: String? = null): String {
        val actualTraceId = traceId ?: UUID.randomUUID().toString()

        val details = mutableMapOf<String, Any?>()
        details["address"] = message.address()
        details["direction"] = "IN"

        val body = message.body()
        if (body is JsonObject) {
            details["messageFields"] = body.fieldNames().joinToString(",")
        }

        return trace(action, details, actualTraceId)
    }

    /**
     * Log a reply to an event bus message
     */
    fun <T> logReply(message: Message<T>, reply: Any, traceId: String): String {
        val details = mutableMapOf<String, Any?>()
        details["address"] = message.address()
        details["direction"] = "REPLY"

        if (reply is JsonObject) {
            details["replyFields"] = reply.fieldNames().joinToString(",")
        }

        return trace("MESSAGE_REPLY", details, traceId)
    }

    /**
     * Log an error
     */
    fun logError(action: String, error: Throwable, details: Map<String, Any?> = emptyMap(), traceId: String? = null): String {
        val actualTraceId = traceId ?: UUID.randomUUID().toString()

        val errorDetails = mutableMapOf<String, Any?>()
        errorDetails["errorType"] = error.javaClass.simpleName
        errorDetails["errorMessage"] = error.message

        errorDetails.putAll(details)

        val logEntry = JsonObject()
            .put("component", component)
            .put("action", action)
            .put("traceId", actualTraceId)
            .put("timestamp", System.currentTimeMillis())

        errorDetails.forEach { (key, value) ->
            if (value != null) {
                logEntry.put(key, value.toString())
            }
        }

        log.error("[ERROR] {}", logEntry.encode())
        return actualTraceId
    }

    /**
     * Log websocket activity
     */
    fun logWebSocketEvent(event: String, connectionId: String?, details: Map<String, Any?> = emptyMap(), traceId: String? = null): String {
        val actualTraceId = traceId ?: UUID.randomUUID().toString()

        val wsDetails = mutableMapOf<String, Any?>()
        wsDetails["event"] = event
        wsDetails["connectionId"] = connectionId

        wsDetails.putAll(details)

        return trace("WEBSOCKET", wsDetails, actualTraceId)
    }

    /**
     * Log a database operation
     */
    fun logDbOperation(operation: String, collection: String, details: Map<String, Any?> = emptyMap(), traceId: String? = null): String {
        val actualTraceId = traceId ?: UUID.randomUUID().toString()

        val dbDetails = mutableMapOf<String, Any?>()
        dbDetails["operation"] = operation
        dbDetails["collection"] = collection

        dbDetails.putAll(details)

        return trace("DATABASE", dbDetails, actualTraceId)
    }

    /**
     * Log a significant state change in a component
     */
    fun logStateChange(entity: String, fromState: String, toState: String, details: Map<String, Any?> = emptyMap(),
                       traceId: String? = null): String {
        val actualTraceId = traceId ?: UUID.randomUUID().toString()

        val stateDetails = mutableMapOf<String, Any?>()
        stateDetails["entity"] = entity
        stateDetails["fromState"] = fromState
        stateDetails["toState"] = toState

        stateDetails.putAll(details)

        return trace("STATE_CHANGE", stateDetails, actualTraceId)
    }

    /**
     * Log performance metrics
     */
    fun logPerformance(operation: String, durationMs: Long, details: Map<String, Any?> = emptyMap(),
                       traceId: String? = null): String {
        val actualTraceId = traceId ?: UUID.randomUUID().toString()

        val perfDetails = mutableMapOf<String, Any?>()
        perfDetails["operation"] = operation
        perfDetails["durationMs"] = durationMs

        perfDetails.putAll(details)

        return trace("PERFORMANCE", perfDetails, actualTraceId)
    }

    /**
     * Create a child logger for a subcomponent
     */
    fun createChildLogger(subcomponent: String): EventLogger {
        return EventLogger("$component.$subcomponent")
    }

    companion object {
        /**
         * Create a logger for a specific class
         */
        fun forClass(clazz: Class<*>): EventLogger {
            return EventLogger(clazz.simpleName)
        }

        /**
         * Create a logger for a specific component
         */
        fun forComponent(component: String): EventLogger {
            return EventLogger(component)
        }
    }
}