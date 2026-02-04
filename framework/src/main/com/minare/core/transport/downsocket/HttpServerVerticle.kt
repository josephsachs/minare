package com.minare.core.transport.downsocket

import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import java.util.concurrent.atomic.AtomicBoolean

/**
 * Dedicated HTTP server verticle that can be deployed by other verticles
 * to handle HTTP traffic without depending on the main application server.
 *
 * This allows for greater separation of concerns and scaling flexibility.
 */
class HttpServerVerticle : CoroutineVerticle() {
    private val log = LoggerFactory.getLogger(HttpServerVerticle::class.java)
    private lateinit var vlog: VerticleLogger

    private var httpServer: HttpServer? = null
    private lateinit var mainRouter: Router
    private val isStarted = AtomicBoolean(false)

    companion object {
        const val DEFAULT_PORT = 8080
        const val DEFAULT_HOST = "0.0.0.0"

        const val ADDRESS_REGISTER_ROUTER = "minare.http.server.register.router"
        const val ADDRESS_SERVER_STARTED = "minare.http.server.started"
        const val ADDRESS_SERVER_STOPPED = "minare.http.server.stopped"
    }

    override suspend fun start() {
        log.info("Starting HttpServerVerticle")

        vlog = VerticleLogger()
        vlog.setVerticle(this)
        vlog.logConfig(config)

        mainRouter = Router.router(vertx)

        vertx.eventBus().consumer<JsonObject>(ADDRESS_REGISTER_ROUTER) { message ->
            CoroutineScope(vertx.dispatcher()).launch {
                val routerId = message.body().getString("routerId")
                val mountPoint = message.body().getString("mountPoint", "/")

                val traceId = vlog.getEventLogger().logReceive(message, "REGISTER_ROUTER")

                try {
                    val reply = JsonObject()
                        .put("success", true)
                        .put("message", "Router registration acknowledged for $routerId at $mountPoint")

                    vlog.getEventLogger().logReply(message, reply, traceId)
                    message.reply(reply)

                    vlog.logStartupStep("ROUTER_REGISTRATION_ACKNOWLEDGED", mapOf(
                        "routerId" to routerId,
                        "mountPoint" to mountPoint
                    ))
                } catch (e: Exception) {
                    vlog.logVerticleError("REGISTER_ROUTER", e)
                    message.fail(500, "Failed to register router: ${e.message}")
                }
            }
        }


        startHttpServer()

        deploymentID?.let {
            vlog.logDeployment(it)
        }

        vlog.logStartupStep("STARTED")
    }

    /**
     * Start the HTTP server with the configured options
     */
    private suspend fun startHttpServer() {
        if (isStarted.get()) {
            log.warn("HTTP server already started")
            return
        }

        try {
            val port = config.getInteger("port", DEFAULT_PORT)
            val host = config.getString("host", DEFAULT_HOST)

            vlog.logStartupStep("STARTING_HTTP_SERVER", mapOf(
                "host" to host,
                "port" to port
            ))

            val options = HttpServerOptions()
                .setHost(host)
                .setPort(port)
                .setTcpKeepAlive(true)
                .setTcpNoDelay(true)

            httpServer = vertx.createHttpServer(options)
                .requestHandler(mainRouter)
                .listen()
                .await()

            isStarted.set(true)

            vlog.logStartupStep("HTTP_SERVER_STARTED", mapOf(
                "actualPort" to httpServer!!.actualPort()
            ))

            vertx.eventBus().publish(
                ADDRESS_SERVER_STARTED, JsonObject()
                .put("port", httpServer!!.actualPort())
                .put("host", host)
            )

            log.info("HTTP server started on {}:{}", host, httpServer!!.actualPort())
        } catch (e: Exception) {
            vlog.logVerticleError("START_HTTP_SERVER", e)
            throw e
        }
    }

    override suspend fun stop() {
        if (httpServer != null) {
            try {
                vlog.logStartupStep("STOPPING_HTTP_SERVER")

                httpServer!!.close().await()
                isStarted.set(false)

                vertx.eventBus().publish(ADDRESS_SERVER_STOPPED, JsonObject())

                vlog.logStartupStep("HTTP_SERVER_STOPPED")
                log.info("HTTP server stopped")
            } catch (e: Exception) {
                vlog.logVerticleError("STOP_HTTP_SERVER", e)
                log.error("Error stopping HTTP server", e)
            }
        }

        vlog.logUndeployment()
    }
}