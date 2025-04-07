package com.minare.utils

import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * Extension of EventLogger specifically for use with Verticles.
 * Provides verticle-specific logging patterns and lifecycle tracking.
 */
class VerticleLogger @Inject constructor() {
    private var verticle: CoroutineVerticle? = null
    private var log: Logger? = null
    private var eventLog: EventLogger? = null

    // Store deployment ID when available
    private var deploymentId: String? = null

    // Store state changes
    private var currentState = "INITIALIZING"

    /**
     * Secondary constructor for direct initialization
     */
    constructor(verticle: CoroutineVerticle) : this() {
        setVerticle(verticle)
    }

    /**
     * Set the verticle reference, which initializes the logger
     */
    fun setVerticle(verticle: CoroutineVerticle) {
        this.verticle = verticle
        this.log = LoggerFactory.getLogger(verticle.javaClass)
        this.eventLog = EventLogger.forClass(verticle.javaClass)
    }

    /**
     * Create an EventBusUtils instance for this verticle
     */
    fun createEventBusUtils(): EventBusUtils {
        checkVerticleInitialized()
        return EventBusUtils(
            verticle!!.vertx,
            verticle!!.vertx.dispatcher(),
            verticle!!.javaClass.simpleName
        )
    }

    /**
     * Log verticle deployment
     */
    fun logDeployment(id: String) {
        checkVerticleInitialized()
        deploymentId = id

        eventLog!!.logStateChange(
            verticle!!.javaClass.simpleName,
            currentState,
            "DEPLOYED",
            mapOf("deploymentId" to id)
        )

        currentState = "DEPLOYED"
        log!!.info("{} deployed with ID: {}", verticle!!.javaClass.simpleName, id)
    }

    /**
     * Log verticle undeployment
     */
    fun logUndeployment() {
        checkVerticleInitialized()
        eventLog!!.logStateChange(
            verticle!!.javaClass.simpleName,
            currentState,
            "UNDEPLOYED",
            mapOf("deploymentId" to (deploymentId ?: "unknown"))
        )

        currentState = "UNDEPLOYED"
        log!!.info("{} undeployed", verticle!!.javaClass.simpleName)
    }

    /**
     * Log an event bus handler registration
     */
    fun logHandlerRegistration(address: String) {
        checkVerticleInitialized()
        eventLog!!.trace(
            "HANDLER_REGISTERED",
            mapOf(
                "address" to address,
                "verticle" to verticle!!.javaClass.simpleName,
                "deploymentId" to (deploymentId ?: "unknown")
            )
        )

        log!!.debug("Registered event bus handler for {}: {}", verticle!!.javaClass.simpleName, address)
    }

    /**
     * Log a verticle startup step
     */
    fun logStartupStep(step: String, details: Map<String, Any?> = emptyMap()) {
        checkVerticleInitialized()
        val stepInfo = mutableMapOf<String, Any?>()
        stepInfo["step"] = step
        stepInfo["verticle"] = verticle!!.javaClass.simpleName
        stepInfo["deploymentId"] = deploymentId
        stepInfo.putAll(details)

        eventLog!!.trace("STARTUP_STEP", stepInfo)
        log!!.info("{} startup step: {}", verticle!!.javaClass.simpleName, step)
    }

    /**
     * Log a verticle error
     */
    fun logVerticleError(action: String, error: Throwable, details: Map<String, Any?> = emptyMap()) {
        checkVerticleInitialized()
        val errorInfo = mutableMapOf<String, Any?>()
        errorInfo["action"] = action
        errorInfo["verticle"] = verticle!!.javaClass.simpleName
        errorInfo["deploymentId"] = deploymentId
        errorInfo.putAll(details)

        eventLog!!.logError("VERTICLE_ERROR", error, errorInfo)
        log!!.error("{} error during {}: {}", verticle!!.javaClass.simpleName, action, error.message, error)
    }

    /**
     * Log a performance metric specific to this verticle
     */
    fun logVerticlePerformance(operation: String, durationMs: Long, details: Map<String, Any?> = emptyMap()) {
        checkVerticleInitialized()
        val perfInfo = mutableMapOf<String, Any?>()
        perfInfo["operation"] = operation
        perfInfo["verticle"] = verticle!!.javaClass.simpleName
        perfInfo["deploymentId"] = deploymentId
        perfInfo.putAll(details)

        eventLog!!.logPerformance("VERTICLE_PERFORMANCE", durationMs, perfInfo)

        if (durationMs > 100) {
            log!!.info("{} operation {} took {}ms", verticle!!.javaClass.simpleName, operation, durationMs)
        }
    }

    /**
     * Get the underlying EventLogger
     */
    fun getEventLogger(): EventLogger {
        checkVerticleInitialized()
        return eventLog!!
    }

    /**
     * Log configuration - must be called from within the verticle
     * where config is accessible
     */
    fun logConfig(config: JsonObject) {
        checkVerticleInitialized()
        val configInfo = mutableMapOf<String, Any?>()
        configInfo["verticle"] = verticle!!.javaClass.simpleName

        val configKeys = config.fieldNames().joinToString(", ")
        configInfo["configKeys"] = configKeys

        eventLog!!.trace("VERTICLE_CONFIG", configInfo)
        log!!.debug("{} configuration: keys=[{}]", verticle!!.javaClass.simpleName, configKeys)
    }

    /**
     * Check if the verticle has been initialized
     */
    private fun checkVerticleInitialized() {
        if (verticle == null) {
            throw IllegalStateException("VerticleLogger not initialized with verticle. Call setVerticle() first.")
        }
    }
}