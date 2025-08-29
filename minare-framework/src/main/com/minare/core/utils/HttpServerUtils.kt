package com.minare.utils

import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

/**
 * Utilities for HTTP server configuration and management.
 * Provides consistent ways to create and configure HTTP servers across verticles.
 */
object HttpServerUtils {
    private val log = LoggerFactory.getLogger(HttpServerUtils::class.java)

    /**
     * Create and start an HTTP server with the given configuration
     *
     * @param vertx Vertx instance
     * @param router Router to handle requests
     * @param host Host to bind to
     * @param port Port to listen on
     * @param options Additional server options
     * @return Future that completes when the server is started
     */
    fun createAndStartHttpServer(
        vertx: Vertx,
        router: Router,
        host: String = "0.0.0.0",
        port: Int,
        options: HttpServerOptions? = null
    ): Future<HttpServer> {
        log.info("Starting HTTP server on $host:$port")

        val serverOptions = options ?: HttpServerOptions()
            .setHost(host)
            .setPort(port)
            .setLogActivity(true)

        return vertx.createHttpServer(serverOptions)
            .requestHandler(router)
            .listen()
            .onSuccess { server ->
                log.info("HTTP server started successfully on port {}", server.actualPort())
            }
            .onFailure { err ->
                log.error("Failed to start HTTP server: {}", err.message, err)
            }
    }

    /**
     * Add standard health endpoint to a router
     *
     * @param router Router to add the endpoint to
     * @param path Health endpoint path (e.g., "/health")
     * @param verticleName Name of the verticle for reporting
     * @param deploymentId Deployment ID for the verticle
     * @param deployedAt Timestamp when the verticle was deployed
     * @param metricsSupplier Supplier of additional metrics to include
     */
    fun addHealthEndpoint(
        router: Router,
        path: String,
        verticleName: String,
        deploymentId: String?,
        deployedAt: Long,
        metricsSupplier: () -> JsonObject
    ) {
        router.get(path).handler { ctx ->
            val healthInfo = JsonObject()
                .put("status", "ok")
                .put("verticle", verticleName)
                .put("deploymentId", deploymentId)
                .put("timestamp", System.currentTimeMillis())
                .put("uptime", System.currentTimeMillis() - deployedAt)
                .put("metrics", metricsSupplier())

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(healthInfo.encode())
        }
    }

    /**
     * Add a simple debug endpoint to a router
     *
     * @param router Router to add the endpoint to
     * @param path Debug endpoint path
     * @param verticleName Name of the verticle for reporting
     */
    fun addDebugEndpoint(
        router: Router,
        path: String,
        verticleName: String
    ) {
        router.get(path).handler { ctx ->
            ctx.response()
                .putHeader("Content-Type", "application/json")
                .end(JsonObject()
                    .put("status", "ok")
                    .put("message", "$verticleName router is working")
                    .put("timestamp", System.currentTimeMillis())
                    .encode())
        }
    }

    /**
     * Create a typical Router for a WebSocket server
     *
     * @param vertx Vertx instance
     * @param verticleName Name of the verticle for logging
     * @param verticleLogger Logger for the verticle
     * @param wsPath WebSocket endpoint path
     * @param healthPath Health endpoint path
     * @param debugPath Debug endpoint path
     * @param deploymentId Deployment ID
     * @param deployedAt Timestamp when deployed
     * @param wsHandler WebSocket handler
     * @param coroutineContext Coroutine context for launching handlers
     * @param metricsSupplier Supplier of metrics for health endpoint
     * @return Configured Router
     */
    fun createWebSocketRouter(
        vertx: Vertx,
        verticleName: String,
        verticleLogger: VerticleLogger,
        wsPath: String,
        healthPath: String,
        debugPath: String,
        deploymentId: String?,
        deployedAt: Long,
        wsHandler: suspend (ServerWebSocket, String) -> Unit,
        coroutineContext: CoroutineContext,
        metricsSupplier: () -> JsonObject
    ): Router {
        val router = Router.router(vertx)
        verticleLogger.logStartupStep("ROUTER_CREATED")

        addDebugEndpoint(router, debugPath, verticleName)
        addHealthEndpoint(
            router, healthPath, verticleName, deploymentId, deployedAt, metricsSupplier
        )

        log.info("Setting up websocket route handler at path: {}", wsPath)
        router.route(wsPath).handler { context ->
            WebSocketUtils.handleWebSocketUpgrade(
                context,
                coroutineContext,
                wsPath,
                verticleLogger,
                wsHandler
            )
        }

        verticleLogger.logStartupStep("ROUTER_INITIALIZED")
        log.info("$verticleName router initialized with routes: $wsPath, $healthPath, $debugPath")

        return router
    }
}