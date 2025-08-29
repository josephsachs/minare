package com.minare.core.frames.coordinator

import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.frames.coordinator.services.MessageQueueOperationConsumer
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
import com.minare.core.frames.coordinator.services.FrameCalculatorService
import com.minare.core.frames.services.WorkerRegistry

/**
 * Admin HTTP interface for the Frame Coordinator.
 * Provides endpoints for infrastructure management and monitoring.
 *
 * IMPORTANT: Port 9090 should be secured by VPC and security group in the infrastructure config
 * Open to the internet at your own risk, or better yet don't
 */
class CoordinatorAdminVerticle @Inject constructor(
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry,
    private val frameCoordinatorState: FrameCoordinatorState,
    private val messageQueueOperationConsumer: MessageQueueOperationConsumer,
    private val frameCalculator: FrameCalculatorService
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(CoordinatorAdminVerticle::class.java)
    private var httpServer: HttpServer? = null

    companion object {
        const val ADMIN_HOST = "0.0.0.0"
        const val ADMIN_PORT = 9090
    }

    override suspend fun start() {
        log.info("Starting CoordinatorAdminVerticle")
        vlog.setVerticle(this)

        val router = createRouter()
        startHttpServer(router)
    }

    private fun createRouter(): Router {
        val router = Router.router(vertx)
        router.route().handler(BodyHandler.create())

        router.get("/health").handler { ctx ->
            val health = JsonObject()
                .put("status", if (frameCoordinatorState.isFrameLoopRunning()) "UP" else "DOWN")
                .put("timestamp", System.currentTimeMillis())

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(health.encode())
        }

        router.get("/status").handler { ctx ->
            val workerCounts = workerRegistry.getWorkerCountByStatus()
            val frameStatus = frameCoordinatorState.getCurrentFrameStatus()
            val consumerMetrics = messageQueueOperationConsumer.getMetrics()

            val status = JsonObject()
                .put("coordinator", JsonObject()
                    .put("sessionStarted", frameCoordinatorState.sessionStartTimestamp > 0)
                    .put("frameStatus", frameStatus))
                .put("workers", JsonObject()
                    .put("expected", workerRegistry.getExpectedWorkerCount())
                    .put("active", workerCounts[WorkerRegistry.WorkerStatus.ACTIVE] ?: 0)
                    .put("pending", workerCounts[WorkerRegistry.WorkerStatus.PENDING] ?: 0)
                    .put("unhealthy", workerCounts[WorkerRegistry.WorkerStatus.UNHEALTHY] ?: 0)
                    .put("removing", workerCounts[WorkerRegistry.WorkerStatus.REMOVING] ?: 0)
                    .put("ready", workerRegistry.getActiveWorkers().size == workerRegistry.getExpectedWorkerCount()))
                .put("buffer", JsonObject()
                    .put("totalOperations", frameCoordinatorState.getTotalBufferedOperations())
                    .put("frameDistribution", frameCoordinatorState.getBufferedOperationCounts())
                .put("consumer", consumerMetrics))

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(status.encode())
        }

        router.get("/workers").handler { ctx ->
            val workers = workerRegistry.getAllWorkers()
            val expectedCount = workerRegistry.getExpectedWorkerCount()
            val activeWorkers = workerRegistry.getActiveWorkers()

            val response = JsonObject()
                .put("expected", expectedCount)
                .put("ready", activeWorkers.size == expectedCount)
                .put("summary", JsonObject()
                    .put("active", workers.values.count { it.status == WorkerRegistry.WorkerStatus.ACTIVE })
                    .put("pending", workers.values.count { it.status == WorkerRegistry.WorkerStatus.PENDING })
                    .put("unhealthy", workers.values.count { it.status == WorkerRegistry.WorkerStatus.UNHEALTHY })
                    .put("removing", workers.values.count { it.status == WorkerRegistry.WorkerStatus.REMOVING })
                )
                .put("workers", JsonArray(
                    workers.map { (id, state) ->
                        JsonObject()
                            .put("id", id)
                            .put("status", state.status.toString())
                            .put("lastHeartbeat", state.lastHeartbeat)
                            .put("heartbeatAge", System.currentTimeMillis() - state.lastHeartbeat)
                            .put("addedAt", state.addedAt)
                    }
                ))

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(response.encode())
        }

        router.get("/frames").handler { ctx ->
            val currentFrame = frameCoordinatorState.getCurrentLogicalFrame()
            val frameInProgress = frameCoordinatorState.frameInProgress
            val lastProcessed = frameCoordinatorState.lastProcessedFrame
            val lastPrepared = frameCoordinatorState.lastPreparedManifest

            val frameStatus = JsonObject()
                .put("running", frameCoordinatorState.isFrameLoopRunning())
                .put("currentWallClockFrame", currentFrame)
                .put("frameInProgress", frameInProgress)
                .put("lastProcessedFrame", lastProcessed)
                .put("lastPreparedManifest", lastPrepared)
                .put("sessionStartTimestamp", frameCoordinatorState.sessionStartTimestamp)
                .put("bufferedOperations", frameCoordinatorState.getBufferedOperationCounts())
                .put("totalBufferedOperations", frameCoordinatorState.getTotalBufferedOperations())

            if (currentFrame >= 0 && frameInProgress >= 0) {
                val processingStatus = frameCalculator.getFrameProcessingStatus(frameInProgress, currentFrame)
                frameStatus
                    .put("frameLag", processingStatus.framesBehind)
                    .put("lagSeverity", processingStatus.lagSeverity.name)
                    .put("isHealthy", processingStatus.isHealthy)
                    .put("recommendedAction", processingStatus.recommendedAction)
            } else {
                frameStatus
                    .put("frameLag", 0)
                    .put("lagSeverity", FrameCalculatorService.LagSeverity.NONE.name)
                    .put("isHealthy", true)
                    .put("recommendedAction", "Session not started")
            }

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(frameStatus.encode())
        }

        router.get("/buffer").handler { ctx ->
            val bufferCounts = frameCoordinatorState.getBufferedOperationCounts()
            val totalBuffered = frameCoordinatorState.getTotalBufferedOperations()

            val bufferStatus = JsonObject()
                .put("totalOperations", totalBuffered)
                .put("frameDistribution", bufferCounts)
                .put("bufferedFrameCount", bufferCounts.size)
                .put("oldestBufferedFrame", bufferCounts.keys.minOrNull() ?: -1)
                .put("newestBufferedFrame", bufferCounts.keys.maxOrNull() ?: -1)

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(bufferStatus.encode())
        }

        router.post("/eventbus").handler { ctx ->
            launch {
                try {
                    val body = ctx.body().asJsonObject()
                    val address = body.getString("address")
                    val message = body.getJsonObject("body")

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

        /**
         * IMPORTANT: Worker add and remove endpoints are the critical step when updating expected
         * workers for the frame coordinator. Infrastructure is expected to add and remove these
         * at application startup and during scaling events.
         */
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