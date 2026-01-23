package com.minare.integration.controller

import com.google.inject.Provider
import com.minare.application.interfaces.AppState
import com.minare.controller.ChannelController
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ContextStore
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Test-specific extension of the framework's ChannelController.
 * Adds application-specific functionality like default channel management.
 */
@Singleton
class TestChannelController @Inject constructor(
    private val appStateProvider: Provider<AppState>
) : ChannelController() {
    private val log = LoggerFactory.getLogger(TestChannelController::class.java)

    companion object {
        private const val DEFAULT_CHANNEL_KEY = "integration.defaultChannel"
    }

    /**
     * Set the default channel ID for this application
     */
    suspend fun setDefaultChannel(channelId: String) {
        log.info("Setting default channel to: {}", channelId)
        appStateProvider.get().set(DEFAULT_CHANNEL_KEY, channelId)
    }

    /**
     * Get the default channel ID for this application
     */
    suspend fun getDefaultChannel(): String? {
        return appStateProvider.get().get(DEFAULT_CHANNEL_KEY)
    }
}