package com.minare.core.transport.adapters

import com.minare.core.transport.interfaces.ProtocolRouter
import com.minare.core.transport.services.HttpServerUtils
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

class WebsocketRouter(
    vertx: Vertx,
    context: CoroutineContext,
    private val logger: VerticleLogger
) : ProtocolRouter<ServerWebSocket>(vertx, context) {
    private val log = LoggerFactory.getLogger(WebsocketRouter::class.java)

    private var router: Router? = null
    private var httpServer: HttpServer? = null

    override fun setupRoutes(
        basePath: String,
        onConnection: suspend (ServerWebSocket, String) -> Unit
    ) {
        val r = Router.router(vertx)

        r.route(basePath).handler { ctx ->
            handleUpgrade(ctx, onConnection)
        }

        HttpServerUtils.addDebugEndpoint(r, "$basePath/debug", "WebsocketRouter")

        router = r
    }

    suspend fun startServer(host: String, port: Int): HttpServer {
        val r = router ?: throw IllegalStateException("Routes not configured; call setupRoutes first")
        val server = HttpServerUtils.createAndStartHttpServer(vertx, r, host, port).await()
        httpServer = server
        return server
    }

    fun addHealthEndpoint(
        path: String,
        verticleName: String,
        deploymentId: String?,
        deployedAt: Long,
        metricsSupplier: () -> JsonObject
    ) {
        val r = router ?: throw IllegalStateException("Routes not configured; call setupRoutes first")
        HttpServerUtils.addHealthEndpoint(r, path, verticleName, deploymentId, deployedAt, metricsSupplier)
    }

    fun getRouter(): Router = router ?: throw IllegalStateException("Routes not configured")

    override fun sendMessage(socket: ServerWebSocket, message: JsonObject) {
        try {
            socket.writeTextMessage(message.encode())
        } catch (e: Exception) {
            log.warn("Failed to send message: {}", e.message)
        }
    }

    override fun sendError(socket: ServerWebSocket, error: Throwable, connectionId: String?) {
        try {
            val errorMessage = JsonObject()
                .put("type", "error")
                .put("code", when (error) {
                    is IllegalArgumentException -> "INVALID_MESSAGE"
                    is IllegalStateException -> "INVALID_STATE"
                    else -> "INTERNAL_ERROR"
                })
                .put("message", error.message ?: "Unknown error")
                .put("timestamp", System.currentTimeMillis())

            socket.writeTextMessage(errorMessage.encode())
        } catch (e: Exception) {
            log.warn("Failed to send error response: {}", e.message)
        }
    }

    override fun sendConfirmation(socket: ServerWebSocket, type: String, connectionId: String) {
        try {
            val confirmation = JsonObject()
                .put("type", type)
                .put("connectionId", connectionId)
                .put("timestamp", System.currentTimeMillis())

            socket.writeTextMessage(confirmation.encode())
        } catch (e: Exception) {
            log.warn("Failed to send confirmation: {}", e.message)
        }
    }

    override fun close(socket: ServerWebSocket): Boolean {
        return try {
            if (!socket.isClosed) {
                socket.close()
            }
            true
        } catch (e: Exception) {
            log.warn("Failed to close socket: {}", e.message)
            false
        }
    }

    override fun isClosed(socket: ServerWebSocket): Boolean {
        return socket.isClosed
    }

    suspend fun stopServer() {
        httpServer?.close()?.await()
        httpServer = null
    }

    private fun handleUpgrade(
        ctx: io.vertx.ext.web.RoutingContext,
        onConnection: suspend (ServerWebSocket, String) -> Unit
    ) {
        val traceId = logger.getEventLogger().trace("WEBSOCKET_ROUTE_ACCESSED", mapOf(
            "path" to ctx.request().path(),
            "remoteAddress" to ctx.request().remoteAddress().toString()
        ))

        ctx.request().toWebSocket()
            .onSuccess { socket ->
                CoroutineScope(context).launch {
                    try {
                        onConnection(socket, traceId)
                    } catch (e: Exception) {
                        logger.logVerticleError("WEBSOCKET_HANDLER", e)
                        close(socket)
                    }
                }
            }
            .onFailure { err ->
                log.error("WebSocket upgrade failed: {}", err.message)
                ctx.response()
                    .setStatusCode(400)
                    .putHeader("Content-Type", "text/plain")
                    .end("WebSocket upgrade failed: ${err.message}")
            }
    }
}