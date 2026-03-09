package com.minare.nodegraph.controller

import com.google.inject.Provider
import com.minare.application.interfaces.AppState
import com.minare.controller.ChannelController
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Example-specific extension of the framework's ChannelController.
 * Adds application-specific functionality like default channel management
 * and the metrics broadcast channel.
 */
@Singleton
class NodeGraphChannelController @Inject constructor(
    private val appStateProvider: Provider<AppState>
) : ChannelController() {
    private val log = LoggerFactory.getLogger(NodeGraphChannelController::class.java)

    companion object {
        private const val DEFAULT_CHANNEL_KEY = "example.defaultChannel"
        private const val METRICS_CHANNEL_KEY = "example.metricsChannel"
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

    /**
     * Create and store the metrics channel.
     * This channel carries no entities — it is used exclusively for
     * broadcasting frame metrics and operation manifests to diagnostic clients.
     */
    suspend fun initializeMetricsChannel(): String {
        val channelId = createChannel()
        log.info("Created metrics channel: {}", channelId)
        appStateProvider.get().set(METRICS_CHANNEL_KEY, channelId)
        return channelId
    }

    /**
     * Get the metrics channel ID, if it has been created.
     */
    suspend fun getMetricsChannel(): String? {
        return appStateProvider.get().get(METRICS_CHANNEL_KEY)
    }
}