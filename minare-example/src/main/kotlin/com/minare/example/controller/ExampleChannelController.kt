package com.minare.example.controller

import com.google.inject.Provider
import com.minare.application.AppState
import com.minare.controller.ChannelController
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ContextStore
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
    contextStore: ContextStore,
    private val appStateProvider: Provider<AppState>
) : ChannelController(channelStore, contextStore) {
    private val log = LoggerFactory.getLogger(ExampleChannelController::class.java)

    companion object {
        private const val DEFAULT_CHANNEL_KEY = "example.defaultChannel"
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