package com.minare.core.transport.services

import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import org.slf4j.LoggerFactory

/**
 * Provider for standardized health and debug endpoints across verticles.
 * Makes health/status reporting consistent throughout the application.
 */
class HealthEndpointProvider(
    private val router: Router,
    private val componentName: String,
    private val deploymentId: String?,
    private val startTime: Long
) {
    private val log = LoggerFactory.getLogger(HealthEndpointProvider::class.java)

    private val metricsProviders = mutableMapOf<String, () -> JsonObject>()
    private val statsProviders = mutableMapOf<String, () -> Any?>()

    /**
     * Register a metrics provider that will contribute to the health output
     *
     * @param name Name of the metrics section
     * @param provider Function that provides a JsonObject of metrics
     * @return This provider for chaining
     */
    fun registerMetricsProvider(name: String, provider: () -> JsonObject): HealthEndpointProvider {
        metricsProviders[name] = provider
        return this
    }

    /**
     * Register a stats provider for a single value
     *
     * @param name Name of the stat
     * @param provider Function that provides the stat value
     * @return This provider for chaining
     */
    fun registerStatProvider(name: String, provider: () -> Any?): HealthEndpointProvider {
        statsProviders[name] = provider
        return this
    }

    /**
     * Add the standard health endpoint to the router
     *
     * @param path Path for the health endpoint
     * @return This provider for chaining
     */
    fun addHealthEndpoint(path: String = "/health"): HealthEndpointProvider {
        router.get(path).handler { ctx ->
            val metrics = JsonObject()

            metricsProviders.forEach { (name, provider) ->
                try {
                    metrics.put(name, provider())
                } catch (e: Exception) {
                    log.warn("Error getting metrics from provider $name", e)
                    metrics.put(name, JsonObject().put("error", e.message))
                }
            }

            val stats = JsonObject()
            statsProviders.forEach { (name, provider) ->
                try {
                    stats.put(name, provider())
                } catch (e: Exception) {
                    log.warn("Error getting stat from provider $name", e)
                    stats.put(name, null)
                }
            }

            val healthInfo = JsonObject()
                .put("status", "ok")
                .put("component", componentName)
                .put("deploymentId", deploymentId)
                .put("timestamp", System.currentTimeMillis())
                .put("uptime", System.currentTimeMillis() - startTime)
                .put("stats", stats)
                .put("metrics", metrics)

            ctx.response()
                .putHeader("content-type", "application/json")
                .end(healthInfo.encode())

            if (Math.random() < 0.05) { // ~5% of requests
                log.debug("Health check accessed: {}", path)
            }
        }

        log.info("Added health endpoint at $path for $componentName")
        return this
    }

    /**
     * Add a debug endpoint to the router
     *
     * @param path Path for the debug endpoint
     * @return This provider for chaining
     */
    fun addDebugEndpoint(path: String = "/debug"): HealthEndpointProvider {
        router.get(path).handler { ctx ->
            ctx.response()
                .putHeader("Content-Type", "application/json")
                .end(
                    JsonObject()
                    .put("status", "ok")
                    .put("component", componentName)
                    .put("message", "$componentName router is working")
                    .put("timestamp", System.currentTimeMillis())
                    .encode())
        }

        log.info("Added debug endpoint at $path for $componentName")
        return this
    }

    /**
     * Add both health and debug endpoints
     *
     * @param healthPath Path for the health endpoint
     * @param debugPath Path for the debug endpoint
     * @return This provider for chaining
     */
    fun addAllEndpoints(healthPath: String = "/health", debugPath: String = "/debug"): HealthEndpointProvider {
        addHealthEndpoint(healthPath)
        addDebugEndpoint(debugPath)
        return this
    }
}