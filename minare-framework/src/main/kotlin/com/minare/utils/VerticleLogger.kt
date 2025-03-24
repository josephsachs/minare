package com.minare.utils

import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * Extension of EventLogger specifically for use with Verticles.
 * Provides verticle-specific logging patterns and lifecycle tracking.
 */
class VerticleLogger(val verticle: CoroutineVerticle) {
    private val verticleName = verticle.javaClass.simpleName
    private val log: Logger = LoggerFactory.getLogger(verticle.javaClass)
    private val eventLog = EventLogger.forClass(verticle.javaClass)

    // Store deployment ID when available
    private var deploymentId: String? = null

    // Store state changes
    private var currentState = "INITIALIZING"

    /**
     * Log verticle deployment
     */
    fun logDeployment(id: String) {
        deploymentId = id

        eventLog.logStateChange(
            verticleName,
            currentState,
            "DEPLOYED",
            mapOf(
                "deploymentId" to id
                // No direct access to config here - use logConfig method instead
            )
        )

        currentState = "DEPLOYED"
        log.info("{} deployed with ID: {}", verticleName, id)
    }

    /**
     * Log verticle undeployment
     */
    fun logUndeployment() {
        eventLog.logStateChange(
            verticleName,
            currentState,
            "UNDEPLOYED",
            mapOf("deploymentId" to (deploymentId ?: "unknown"))
        )

        currentState = "UNDEPLOYED"
        log.info("{} undeployed", verticleName)
    }

    /**
     * Log an event bus handler registration
     */
    fun logHandlerRegistration(address: String) {
        eventLog.trace(
            "HANDLER_REGISTERED",
            mapOf(
                "address" to address,
                "verticle" to verticleName,
                "deploymentId" to (deploymentId ?: "unknown")
            )
        )

        log.debug("Registered event bus handler for {}: {}", verticleName, address)
    }

    /**
     * Log a verticle startup step
     */
    fun logStartupStep(step: String, details: Map<String, Any?> = emptyMap()) {
        val stepInfo = mutableMapOf<String, Any?>()
        stepInfo["step"] = step
        stepInfo["verticle"] = verticleName
        stepInfo["deploymentId"] = deploymentId
        stepInfo.putAll(details)

        eventLog.trace("STARTUP_STEP", stepInfo)
        log.info("{} startup step: {}", verticleName, step)
    }

    /**
     * Log a verticle error
     */
    fun logVerticleError(action: String, error: Throwable, details: Map<String, Any?> = emptyMap()) {
        val errorInfo = mutableMapOf<String, Any?>()
        errorInfo["action"] = action
        errorInfo["verticle"] = verticleName
        errorInfo["deploymentId"] = deploymentId
        errorInfo.putAll(details)

        eventLog.logError("VERTICLE_ERROR", error, errorInfo)
        log.error("{} error during {}: {}", verticleName, action, error.message, error)
    }

    /**
     * Log a performance metric specific to this verticle
     */
    fun logVerticlePerformance(operation: String, durationMs: Long, details: Map<String, Any?> = emptyMap()) {
        val perfInfo = mutableMapOf<String, Any?>()
        perfInfo["operation"] = operation
        perfInfo["verticle"] = verticleName
        perfInfo["deploymentId"] = deploymentId
        perfInfo.putAll(details)

        eventLog.logPerformance("VERTICLE_PERFORMANCE", durationMs, perfInfo)

        // Only log to standard logger if performance is notable
        if (durationMs > 100) {
            log.info("{} operation {} took {}ms", verticleName, operation, durationMs)
        }
    }

    /**
     * Create an EventBusUtils instance for this verticle
     */
    fun createEventBusUtils(): EventBusUtils {
        return EventBusUtils(
            verticle.vertx,
            verticle.vertx.dispatcher(),
            verticleName
        )
    }

    /**
     * Get the underlying EventLogger
     */
    fun getEventLogger(): EventLogger {
        return eventLog
    }

    /**
     * Log configuration - must be called from within the verticle
     * where config is accessible
     */
    fun logConfig(config: JsonObject) {
        val configInfo = mutableMapOf<String, Any?>()
        configInfo["verticle"] = verticleName

        // Extract just the config keys to avoid logging sensitive values
        val configKeys = config.fieldNames().joinToString(", ")
        configInfo["configKeys"] = configKeys

        eventLog.trace("VERTICLE_CONFIG", configInfo)
        log.debug("{} configuration: keys=[{}]", verticleName, configKeys)
    }
}