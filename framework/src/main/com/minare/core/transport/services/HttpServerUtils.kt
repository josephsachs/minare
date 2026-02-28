package com.minare.core.transport.services

import io.vertx.core.Future
import io.vertx.core.Vertx
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import org.slf4j.LoggerFactory

object HttpServerUtils {
    private val log = LoggerFactory.getLogger(HttpServerUtils::class.java)

    fun createAndStartHttpServer(
        vertx: Vertx,
        router: Router,
        host: String = "0.0.0.0",
        port: Int,
        options: HttpServerOptions? = null
    ): Future<HttpServer> {
        val serverOptions = options ?: HttpServerOptions()
            .setHost(host)
            .setPort(port)
            .setLogActivity(true)

        return vertx.createHttpServer(serverOptions)
            .requestHandler(router)
            .listen()
            .onSuccess { server ->
                log.info("HTTP server started on port {}", server.actualPort())
            }
            .onFailure { err ->
                log.error("Failed to start HTTP server: {}", err.message, err)
            }
    }

    fun addHealthEndpoint(
        router: Router,
        path: String,
        verticleName: String,
        deploymentId: String?,
        deployedAt: Long,
        metricsSupplier: () -> JsonObject
    ) {
        router.get(path).handler { ctx ->
            ctx.response()
                .putHeader("content-type", "application/json")
                .end(JsonObject()
                    .put("status", "ok")
                    .put("verticle", verticleName)
                    .put("deploymentId", deploymentId)
                    .put("timestamp", System.currentTimeMillis())
                    .put("uptime", System.currentTimeMillis() - deployedAt)
                    .put("metrics", metricsSupplier())
                    .encode()
                )
        }
    }

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
                    .encode()
                )
        }
    }
}