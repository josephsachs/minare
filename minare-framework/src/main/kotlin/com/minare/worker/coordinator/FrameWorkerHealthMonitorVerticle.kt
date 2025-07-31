package com.minare.worker.coordinator

import com.hazelcast.core.HazelcastInstance
import com.minare.time.FrameConfiguration
import com.minare.utils.VerticleLogger
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.RedisAPI
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * Health monitoring verticle for the frame processing system.
 * Runs health checks during the grace period and signals the coordinator
 * whether to proceed with frame processing or abort.
 *
 * This runs independently from the coordinator to avoid blocking
 * frame manifest preparation.
 */
class FrameWorkerHealthMonitorVerticle @Inject constructor(
    private val vlog: VerticleLogger,
    private val frameConfig: FrameConfiguration,
    private val workerRegistry: WorkerRegistry,
    private val hazelcastInstance: HazelcastInstance,
    private val redisAPI: RedisAPI
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(FrameWorkerHealthMonitorVerticle::class.java)

    companion object {
        // Event bus addresses
        const val ADDRESS_HEALTH_CHECK_REQUEST = "minare.health.check.request"
        const val ADDRESS_HEALTH_CHECK_RESULT = "minare.health.check.result"

        // Health check configuration
        const val WORKER_HEARTBEAT_TIMEOUT = 15000L
        const val HAZELCAST_CHECK_TIMEOUT = 5000L
        const val REDIS_CHECK_TIMEOUT = 3000L
    }

    override suspend fun start() {
        log.info("Starting FrameHealthMonitorVerticle")
        vlog.setVerticle(this)

        // Listen for health check requests
        vertx.eventBus().consumer<JsonObject>(ADDRESS_HEALTH_CHECK_REQUEST) { message ->
            launch {
                val frameStartTime = message.body().getLong("frameStartTime")
                performHealthCheck(frameStartTime)
            }
        }
    }

    /**
     * Perform comprehensive health check for frame processing.
     * Checks workers, Hazelcast, and Redis availability.
     */
    private suspend fun performHealthCheck(frameStartTime: Long) {
        val checkStartTime = System.currentTimeMillis()
        log.debug("Starting health check for frame {}", frameStartTime)

        try {
            // 1. Update and check worker health
            val workerHealthResult = checkWorkerHealth()

            // 2. Check Hazelcast cluster health
            val hazelcastHealthResult = checkHazelcastHealth()

            // 3. Check Redis connectivity
            val redisHealthResult = checkRedisHealth()

            // Compile results
            val allHealthy = workerHealthResult.healthy &&
                    hazelcastHealthResult.healthy &&
                    redisHealthResult.healthy

            val checkDuration = System.currentTimeMillis() - checkStartTime

            val result = JsonObject()
                .put("frameStartTime", frameStartTime)
                .put("healthy", allHealthy)
                .put("checkDuration", checkDuration)
                .put("workers", workerHealthResult.toJson())
                .put("hazelcast", hazelcastHealthResult.toJson())
                .put("redis", redisHealthResult.toJson())
                .put("timestamp", System.currentTimeMillis())

            if (!allHealthy) {
                log.warn("Health check failed for frame {}: {}", frameStartTime, result.encode())
            } else {
                log.debug("Health check passed for frame {} in {}ms", frameStartTime, checkDuration)
            }

            // Send result back to coordinator
            vertx.eventBus().send(ADDRESS_HEALTH_CHECK_RESULT, result)

        } catch (e: Exception) {
            log.error("Error during health check for frame {}", frameStartTime, e)

            // Send failure result
            val errorResult = JsonObject()
                .put("frameStartTime", frameStartTime)
                .put("healthy", false)
                .put("error", e.message)
                .put("timestamp", System.currentTimeMillis())

            vertx.eventBus().send(ADDRESS_HEALTH_CHECK_RESULT, errorResult)
        }
    }

    /**
     * Check health of all registered workers.
     */
    private suspend fun checkWorkerHealth(): HealthCheckResult {
        // Update worker health based on heartbeats
        workerRegistry.updateWorkerHealth(WORKER_HEARTBEAT_TIMEOUT)

        val activeWorkers = workerRegistry.getActiveWorkers()

        // For now, we consider it healthy if we have any active workers
        // In the future, we might want to check against a minimum threshold
        val healthy = activeWorkers.isNotEmpty()

        return HealthCheckResult(
            component = "workers",
            healthy = healthy,
            details = JsonObject()
                .put("activeCount", activeWorkers.size)
                .put("activeWorkers", activeWorkers.toList())
        )
    }

    /**
     * Check Hazelcast cluster health.
     */
    private suspend fun checkHazelcastHealth(): HealthCheckResult {
        return try {
            val clusterState = hazelcastInstance.cluster.clusterState
            val memberCount = hazelcastInstance.cluster.members.size

            val healthy = clusterState.toString() == "ACTIVE" && memberCount > 0

            HealthCheckResult(
                component = "hazelcast",
                healthy = healthy,
                details = JsonObject()
                    .put("clusterState", clusterState.toString())
                    .put("memberCount", memberCount)
                    .put("localMember", hazelcastInstance.cluster.localMember.address.toString())
            )
        } catch (e: Exception) {
            HealthCheckResult(
                component = "hazelcast",
                healthy = false,
                details = JsonObject()
                    .put("error", e.message)
            )
        }
    }

    /**
     * Check Redis connectivity.
     */
    private suspend fun checkRedisHealth(): HealthCheckResult {
        return try {
            // Simple PING command with timeout
            val pingPromise = io.vertx.core.Promise.promise<io.vertx.redis.client.Response>()

            redisAPI.ping(emptyList(), pingPromise)

            // Add timeout
            val timeoutId = vertx.setTimer(REDIS_CHECK_TIMEOUT) {
                pingPromise.tryFail("Redis ping timeout")
            }

            val response = pingPromise.future().await()
            vertx.cancelTimer(timeoutId)

            val healthy = response.toString() == "PONG"

            HealthCheckResult(
                component = "redis",
                healthy = healthy,
                details = JsonObject()
                    .put("response", response.toString())
            )
        } catch (e: Exception) {
            HealthCheckResult(
                component = "redis",
                healthy = false,
                details = JsonObject()
                    .put("error", e.message)
            )
        }
    }

    /**
     * Data class for health check results.
     */
    private data class HealthCheckResult(
        val component: String,
        val healthy: Boolean,
        val details: JsonObject
    ) {
        fun toJson(): JsonObject = JsonObject()
            .put("component", component)
            .put("healthy", healthy)
            .put("details", details)
    }

    override suspend fun stop() {
        log.info("Stopping FrameHealthMonitorVerticle")
        super.stop()
    }
}