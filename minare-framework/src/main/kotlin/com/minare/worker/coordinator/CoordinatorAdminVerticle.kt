package com.minare.worker.coordinator

import com.minare.utils.VerticleLogger
import com.minare.worker.coordinator.events.InfraAddWorkerEvent
import com.minare.worker.coordinator.events.InfraRemoveWorkerEvent
import io.vertx.core.http.HttpServer
import io.vertx.core.http.HttpServerOptions
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * Admin HTTP interface for the Frame Coordinator.
 * Provides endpoints for infrastructure management and monitoring.
 *
 * This runs on port 9090 and should be firewalled from public access.
 */
class CoordinatorAdminVerticle @Inject constructor(
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry,
    private val frameCoordinatorState: FrameCoordinatorState
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(CoordinatorAdminVerticle::class.java)
    private var httpServer: HttpServer? = null

    companion object {
        const val ADMIN_PORT = 9090
        const val ADMIN_HOST = "0.0.0.0"
    }

    override suspend fun start() {
        log.info("Starting CoordinatorAdminVerticle")
        vlog.setVerticle(this)

        val router = createRouter()
        startHttpServer(router)

        log.info("CoordinatorAdminVerticle started successfully")
    }

    private fun createRouter(): Router {
        val router = Router.router(vertx)

        // Add body handler for POST requests
        router.route().handler(BodyHandler.create())

        // Health endpoint
        router.get("/health").handler { ctx ->
            val health = JsonObject()
                .put("status", "ok")
                .put("component", "coordinator-admin")
                .put("timestamp", System.currentTimeMillis())
                .put("frameLoop", JsonObject()
                    .put("running", frameCoordinatorState.isFrameLoopRunning())
                    .put("paused", frameCoordinatorState.isPaused)
                    .put("currentFrame", frameCoordinatorState.currentFrameStart)
                )
                .put("workers", JsonObject()
                    .put("active", workerRegistry.getActiveWorkers().size)
                    .put("total", workerRegistry.getAllWorkers().size)
                )

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(health.encode())
        }

        // Event bus endpoint - forwards messages to the event bus
        router.post("/eventbus").handler { ctx ->
            launch {
                try {
                    val body = ctx.body().asJsonObject()
                    val address = body.getString("address")
                    val message = body.getJsonObject("body")

                    if (address == null || message == null) {
                        ctx.response()
                            .setStatusCode(400)
                            .end(JsonObject()
                                .put("error", "Missing required fields: address, body")
                                .encode()
                            )
                        return@launch
                    }

                    // Send to event bus and await reply
                    val reply = vertx.eventBus()
                        .request<JsonObject>(address, message)
                        .await()

                    ctx.response()
                        .putHeader("content-type", "application/json")
                        .end(reply.body().encode())

                } catch (e: Exception) {
                    log.error("Error handling eventbus request", e)
                    ctx.response()
                        .setStatusCode(500)
                        .end(JsonObject()
                            .put("error", e.message)
                            .encode()
                        )
                }
            }
        }

        // Worker management endpoints
        router.post("/workers/add").handler { ctx ->
            launch {
                try {
                    val body = ctx.body().asJsonObject()
                    val workerId = body.getString("workerId")

                    if (workerId == null) {
                        ctx.response()
                            .setStatusCode(400)
                            .end(JsonObject()
                                .put("error", "Missing required field: workerId")
                                .encode()
                            )
                        return@launch
                    }

                    // Send add worker command
                    val reply = vertx.eventBus()
                        .request<JsonObject>(
                            InfraAddWorkerEvent.ADDRESS_INFRA_ADD_WORKER,
                            JsonObject().put("workerId", workerId)
                        )
                        .await()

                    ctx.response()
                        .putHeader("content-type", "application/json")
                        .end(reply.body().encode())

                } catch (e: Exception) {
                    ctx.response()
                        .setStatusCode(500)
                        .end(JsonObject()
                            .put("error", e.message)
                            .encode()
                        )
                }
            }
        }

        router.post("/workers/remove").handler { ctx ->
            launch {
                try {
                    val body = ctx.body().asJsonObject()
                    val workerId = body.getString("workerId")

                    if (workerId == null) {
                        ctx.response()
                            .setStatusCode(400)
                            .end(JsonObject()
                                .put("error", "Missing required field: workerId")
                                .encode()
                            )
                        return@launch
                    }

                    // Send remove worker command
                    val reply = vertx.eventBus()
                        .request<JsonObject>(
                            InfraRemoveWorkerEvent.ADDRESS_INFRA_REMOVE_WORKER,
                            JsonObject().put("workerId", workerId)
                        )
                        .await()

                    ctx.response()
                        .putHeader("content-type", "application/json")
                        .end(reply.body().encode())

                } catch (e: Exception) {
                    ctx.response()
                        .setStatusCode(500)
                        .end(JsonObject()
                            .put("error", e.message)
                            .encode()
                        )
                }
            }
        }

        // Worker status endpoint
        router.get("/workers").handler { ctx ->
            val workers = workerRegistry.getAllWorkers()
            val response = JsonObject()
                .put("workers", JsonArray(workers.map { (id, state) ->
                    JsonObject()
                        .put("id", id)
                        .put("status", state.status.toString())
                        .put("lastHeartbeat", state.lastHeartbeat)
                        .put("addedAt", state.addedAt)
                }))
                .put("summary", JsonObject()
                    .put("total", workers.size)
                    .put("active", workers.values.count { it.status == WorkerRegistry.WorkerStatus.ACTIVE })
                    .put("pending", workers.values.count { it.status == WorkerRegistry.WorkerStatus.PENDING })
                    .put("unhealthy", workers.values.count { it.status == WorkerRegistry.WorkerStatus.UNHEALTHY })
                )

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(response.encode())
        }

        // Frame status endpoint
        router.get("/frames").handler { ctx ->
            val frameStatus = JsonObject()
                .put("running", frameCoordinatorState.isFrameLoopRunning())
                .put("paused", frameCoordinatorState.isPaused)
                .put("currentFrameStart", frameCoordinatorState.currentFrameStart)
                .put("bufferedOperations", frameCoordinatorState.getBufferedOperationCounts())
                .put("frameCompletions", frameCoordinatorState.getFrameCompletionStatus().size)

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(frameStatus.encode())
        }

        // Frame control endpoints
        router.post("/frames/pause").handler { ctx ->
            frameCoordinatorState.isPaused = true
            ctx.response()
                .putHeader("content-type", "application/json")
                .end(JsonObject()
                    .put("success", true)
                    .put("message", "Frame processing paused")
                    .encode()
                )
        }

        router.post("/frames/resume").handler { ctx ->
            frameCoordinatorState.isPaused = false
            ctx.response()
                .putHeader("content-type", "application/json")
                .end(JsonObject()
                    .put("success", true)
                    .put("message", "Frame processing resumed")
                    .encode()
                )
        }

        return router
    }

    private suspend fun startHttpServer(router: Router) {
        val options = HttpServerOptions()
            .setHost(ADMIN_HOST)
            .setPort(ADMIN_PORT)
            .setLogActivity(true)

        httpServer = vertx.createHttpServer(options)
            .requestHandler(router)
            .listen()
            .await()

        log.info("Admin HTTP server started on {}:{}", ADMIN_HOST, httpServer!!.actualPort())
    }

    override suspend fun stop() {
        httpServer?.close()?.await()
        log.info("Admin HTTP server stopped")
    }
}