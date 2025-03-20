package com.minare.example.controller

import com.minare.persistence.ChannelStore
import com.minare.persistence.ContextStore
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Example-specific extension of the framework's ChannelController.
 * Adds application-specific functionality like default channel management.
 */
@Singleton
class ExampleChannelController @Inject constructor(
    channelStore: ChannelStore,
    contextStore: ContextStore
) : ChannelController(channelStore, contextStore) {
    private val log = LoggerFactory.getLogger(ExampleChannelController::class.java)

    // Store the default channel ID - this is application-specific
    private var defaultChannelId: String? = null

    /**
     * Set the default channel ID for this application
     */
    fun setDefaultChannel(channelId: String) {
        log.info("Setting default channel to: {}", channelId)
        defaultChannelId = channelId
    }

    /**
     * Get the default channel ID for this application
     */
    fun getDefaultChannel(): String? {
        return defaultChannelId
    }
}