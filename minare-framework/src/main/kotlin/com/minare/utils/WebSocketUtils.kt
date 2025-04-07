package com.minare.utils

import com.minare.cache.ConnectionCache
import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.RoutingContext
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

/**
 * Utilities for handling WebSocket connections in a consistent way.
 * Abstracts common patterns for WebSocket handling, error management, and tracing.
 */
object WebSocketUtils {
    private val log = LoggerFactory.getLogger(WebSocketUtils::class.java)

    /**
     * Handle upgrade to WebSocket with consistent error handling and tracing
     *
     * @param context The routing context
     * @param coroutineContext Coroutine context to launch handlers in
     * @param path The path being accessed
     * @param logger The verticle logger for tracing
     * @param onSuccess Handler for successful WebSocket upgrade
     */
    fun handleWebSocketUpgrade(
        context: RoutingContext,
        coroutineContext: CoroutineContext,
        path: String,
        logger: VerticleLogger,
        onSuccess: suspend (ServerWebSocket, String) -> Unit
    ) {
        val traceId = logger.getEventLogger().trace("WEBSOCKET_ROUTE_ACCESSED", mapOf(
            "path" to path,
            "remoteAddress" to context.request().remoteAddress().toString()
        ))

        context.request().toWebSocket()
            .onSuccess { socket ->
                log.info("WebSocket upgrade successful for client: {}", socket.remoteAddress())

                CoroutineScope(coroutineContext).launch {
                    try {
                        logger.getEventLogger().trace("WEBSOCKET_UPGRADED", mapOf(
                            "socketId" to socket.textHandlerID()
                        ), traceId)

                        onSuccess(socket, traceId)
                    } catch (e: Exception) {
                        logger.logVerticleError("WEBSOCKET_HANDLER", e, mapOf(
                            "socketId" to socket.textHandlerID()
                        ))

                        try {
                            if (!socket.isClosed()) {
                                socket.close()
                            }
                        } catch (closeEx: Exception) {
                            logger.logVerticleError("WEBSOCKET_CLOSE", closeEx, mapOf(
                                "socketId" to socket.textHandlerID()
                            ))
                        }
                    }
                }
            }
            .onFailure { err ->
                log.error("WebSocket upgrade failed: {}", err.message, err)
                logger.logVerticleError("WEBSOCKET_UPGRADE", err)
                context.response()
                    .setStatusCode(400)
                    .putHeader("Content-Type", "text/plain")
                    .end("WebSocket upgrade failed: ${err.message}")
            }
    }

    /**
     * Send an error response to a WebSocket client
     *
     * @param websocket The WebSocket to send the error to
     * @param error The error to report
     * @param connectionId Optional connection ID for error context
     * @param logger Verticle logger for error reporting
     */
    fun sendErrorResponse(
        websocket: ServerWebSocket,
        error: Throwable,
        connectionId: String?,
        logger: VerticleLogger
    ) {
        logger.logVerticleError("WEBSOCKET_ERROR", error, mapOf(
            "connectionId" to (connectionId ?: "unknown"),
            "remoteAddress" to websocket.remoteAddress().toString()
        ))

        try {
            val errorMessage = JsonObject()
                .put("type", "error")
                .put("message", error.message)
                .put("timestamp", System.currentTimeMillis())

            websocket.writeTextMessage(errorMessage.encode())
        } catch (e: Exception) {
            logger.logVerticleError("ERROR_NOTIFICATION", e, mapOf(
                "connectionId" to (connectionId ?: "unknown")
            ))
        } finally {
            if (connectionId == null && !websocket.isClosed()) {
                websocket.close()
            }
        }
    }

    /**
     * Get connection ID for a socket, checking both command and update sockets
     *
     * @param websocket The WebSocket to check
     * @param connectionCache Connection cache to look up in
     * @return The connection ID or null if not found
     */
    fun getConnectionIdForSocket(
        websocket: ServerWebSocket,
        connectionCache: ConnectionCache
    ): String? {
        val commandId = connectionCache.getConnectionIdForCommandSocket(websocket)
        if (commandId != null) return commandId

        return connectionCache.getConnectionIdForUpdateSocket(websocket)
    }

    /**
     * Create and send a typed confirmation message
     *
     * @param websocket The WebSocket to send to
     * @param confirmationType The type of confirmation (e.g. "connection_confirm")
     * @param connectionId The connection ID to include
     * @param extraFields Optional extra fields to include in the message
     */
    fun sendConfirmation(
        websocket: ServerWebSocket,
        confirmationType: String,
        connectionId: String,
        extraFields: Map<String, Any?> = emptyMap()
    ) {
        val confirmation = JsonObject()
            .put("type", confirmationType)
            .put("connectionId", connectionId)
            .put("timestamp", System.currentTimeMillis())


        extraFields.forEach { (key, value) ->
            if (value != null) {
                confirmation.put(key, value)
            }
        }

        websocket.writeTextMessage(confirmation.encode())
    }
}