package com.minare.core.storage.services

import com.minare.application.config.FrameworkConfig
import com.minare.core.operation.interfaces.MessageQueue
import com.minare.core.operation.adapters.KafkaMessageQueue
import io.vertx.kotlin.coroutines.await
import io.vertx.redis.client.RedisAPI
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Handles initialization and optional reset of all stateful services.
 * Checks RESET_STATE environment variable to determine if data should be cleared.
 */
@Singleton
class StateInitializer @Inject constructor(
    private val frameworkConfig: FrameworkConfig,
    private val databaseInitializer: DatabaseInitializer,
    private val redisAPI: RedisAPI,
    private val messageQueue: MessageQueue
) {
    private val log = LoggerFactory.getLogger(StateInitializer::class.java)

    /**
     * Initialize all stateful services, optionally resetting them first
     */
    suspend fun initialize() {
        val resetState = checkResetStateFlag()

        if (resetState) {
            log.warn("RESET_STATE=true - Resetting all application state")
            resetAllState()
        }

        // Initialize database (DatabaseInitializer already handles RESET_DB internally)
        databaseInitializer.initialize()
        log.info("Database initialized")

        // Any other initialization logic can go here
        log.info("State initialization completed")
    }

    /**
     * Check if the RESET_STATE environment variable is set to true
     */
    private fun checkResetStateFlag(): Boolean {
        return frameworkConfig.development.resetData
    }

    /**
     * Reset all stateful services
     */
    private suspend fun resetAllState() {
        try {
            // Reset Redis
            resetRedis()

            // Reset Kafka topics
            resetKafkaTopics()

            log.info("All state reset completed")
        } catch (e: Exception) {
            log.error("Failed to reset state", e)
            throw e
        }
    }

    /**
     * Flush all Redis data
     */
    private suspend fun resetRedis() {
        try {
            log.info("Flushing Redis...")
            redisAPI.flushdb(listOf()).await()
            log.info("Redis flushed successfully")
        } catch (e: Exception) {
            log.error("Failed to flush Redis", e)
            throw e
        }
    }

    /**
     * Delete all Kafka topics
     */
    private suspend fun resetKafkaTopics() {
        // Only proceed if we have a KafkaMessageQueue
        if (messageQueue !is KafkaMessageQueue) {
            log.debug("MessageQueue is not Kafka, skipping topic reset")
            return
        }

        try {
            log.info("Resetting Kafka topics...")

            // We need to expose the admin client or add a reset method to KafkaMessageQueue
            // For now, we'll need to add a method to KafkaMessageQueue
            messageQueue.resetAllTopics()

            log.info("Kafka topics reset successfully")
        } catch (e: Exception) {
            log.error("Failed to reset Kafka topics", e)
            throw e
        }
    }
}