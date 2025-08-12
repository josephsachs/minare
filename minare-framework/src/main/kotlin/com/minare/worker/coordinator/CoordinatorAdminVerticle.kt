package com.minare.worker.coordinator

import com.minare.time.FrameCalculator
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
 * Enhanced with comprehensive monitoring for event-driven coordination.
 * This runs on port 9090 and should be firewalled from public access.
 */
class CoordinatorAdminVerticle @Inject constructor(
    private val vlog: VerticleLogger,
    private val workerRegistry: WorkerRegistry,
    private val frameCoordinatorState: FrameCoordinatorState,
    private val frameRecoveryManager: FrameRecoveryManager,
    private val messageQueueOperationConsumer: MessageQueueOperationConsumer,
    private val frameCalculator: FrameCalculator
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

        // Health check endpoint
        router.get("/health").handler { ctx ->
            val health = JsonObject()
                .put("status", if (frameCoordinatorState.isFrameLoopRunning()) "UP" else "DOWN")
                .put("timestamp", System.currentTimeMillis())

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(health.encode())
        }

        // Comprehensive status endpoint
        router.get("/status").handler { ctx ->
            val workerCounts = workerRegistry.getWorkerCountByStatus()
            val frameStatus = frameCoordinatorState.getCurrentFrameStatus()
            val recoveryStatus = frameRecoveryManager.getRecoveryStatus()
            val consumerMetrics = messageQueueOperationConsumer.getMetrics()

            val status = JsonObject()
                .put("coordinator", JsonObject()
                    .put("state", if (frameCoordinatorState.isPaused) "PAUSED" else "RUNNING")
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
                    .put("approachingLimit", frameCoordinatorState.isApproachingBufferLimit()))
                .put("recovery", recoveryStatus)
                .put("consumer", consumerMetrics)

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(status.encode())
        }

        // Worker status with enhanced details
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

        // Frame processing status
        router.get("/frames").handler { ctx ->
            val currentFrame = frameCoordinatorState.getCurrentLogicalFrame()
            val frameInProgress = frameCoordinatorState.frameInProgress
            val lastProcessed = frameCoordinatorState.lastProcessedFrame
            val lastPrepared = frameCoordinatorState.lastPreparedManifest

            val frameStatus = JsonObject()
                .put("running", frameCoordinatorState.isFrameLoopRunning())
                .put("paused", frameCoordinatorState.isPaused)
                .put("currentWallClockFrame", currentFrame)
                .put("frameInProgress", frameInProgress)
                .put("lastProcessedFrame", lastProcessed)
                .put("lastPreparedManifest", lastPrepared)
                .put("sessionStartTimestamp", frameCoordinatorState.sessionStartTimestamp)
                .put("bufferedOperations", frameCoordinatorState.getBufferedOperationCounts())
                .put("totalBufferedOperations", frameCoordinatorState.getTotalBufferedOperations())

            // Add rich frame processing status if session is active
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
                    .put("lagSeverity", FrameCalculator.LagSeverity.NONE.name)
                    .put("isHealthy", true)
                    .put("recommendedAction", "Session not started")
            }

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(frameStatus.encode())
        }

        // Buffer status endpoint
        router.get("/buffer").handler { ctx ->
            val bufferCounts = frameCoordinatorState.getBufferedOperationCounts()
            val totalBuffered = frameCoordinatorState.getTotalBufferedOperations()

            val bufferStatus = JsonObject()
                .put("totalOperations", totalBuffered)
                .put("approachingLimit", frameCoordinatorState.isApproachingBufferLimit())
                .put("isPaused", frameCoordinatorState.isPaused)
                .put("frameDistribution", bufferCounts)
                .put("bufferedFrameCount", bufferCounts.size)
                .put("oldestBufferedFrame", bufferCounts.keys.minOrNull() ?: -1)
                .put("newestBufferedFrame", bufferCounts.keys.maxOrNull() ?: -1)

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(bufferStatus.encode())
        }

        // Event bus message endpoint (existing)
        router.post("/eventbus").handler { ctx ->
            launch {
                try {
                    val body = ctx.body().asJsonObject()
                    val address = body.getString("address")
                    val message = body.getJsonObject("body")

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

        // Frame control endpoints
        router.post("/frames/pause").handler { ctx ->
            launch {
                val reason = ctx.body().asJsonObject()?.getString("reason") ?: "Manual pause via admin API"
                frameRecoveryManager.pauseFrameProcessing(reason)

                ctx.response()
                    .putHeader("content-type", "application/json")
                    .end(JsonObject()
                        .put("success", true)
                        .put("message", "Frame processing pause requested")
                        .put("reason", reason)
                        .encode()
                    )
            }
        }

        router.post("/frames/resume").handler { ctx ->
            launch {
                val reason = ctx.body().asJsonObject()?.getString("reason") ?: "Manual resume via admin API"
                frameRecoveryManager.requestFrameResume(reason)

                ctx.response()
                    .putHeader("content-type", "application/json")
                    .end(JsonObject()
                        .put("success", true)
                        .put("message", "Frame processing resume requested")
                        .put("reason", reason)
                        .put("note", "Resume will occur when all workers are ready")
                        .encode()
                    )
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