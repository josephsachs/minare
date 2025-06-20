package com.minare.worker.upsocket.config

import com.google.inject.PrivateModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.minare.cache.ConnectionCache
import com.minare.controller.ConnectionController
import com.minare.persistence.ChannelStore
import com.minare.persistence.ConnectionStore
import com.minare.utils.ConnectionTracker
import com.minare.utils.EventBusUtils
import com.minare.utils.HeartbeatManager
import com.minare.utils.VerticleLogger
import com.minare.worker.upsocket.CommandMessageHandler
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import kotlinx.coroutines.CoroutineScope
import com.minare.worker.upsocket.UpSocketVerticle
import com.minare.worker.upsocket.ConnectionLifecycle
import com.minare.worker.upsocket.events.*
import com.minare.worker.upsocket.handlers.CloseHandler
import com.minare.worker.upsocket.handlers.MessageHandler
import com.minare.worker.upsocket.handlers.ReconnectionHandler
import kotlin.coroutines.CoroutineContext

/**
 * Specialized Guice module for UpSocketVerticle and its dependencies.
 * This module provides all the necessary components within the UpSocketVerticle's scope.
 */
class UpSocketVerticleModule : PrivateModule() {

    override fun configure() {
        bind(UpSocketVerticle::class.java)

        bind(EntitySyncEvent::class.java).`in`(Singleton::class.java)
        bind(ConnectionCleanupEvent::class.java).`in`(Singleton::class.java)
        bind(ChannelCleanupEvent::class.java).`in`(Singleton::class.java)
        bind(UpSocketCleanupEvent::class.java).`in`(Singleton::class.java)
        bind(UpSocketInitEvent::class.java).`in`(Singleton::class.java)

        bind(CloseHandler::class.java).`in`(Singleton::class.java)
        bind(MessageHandler::class.java).`in`(Singleton::class.java)
        bind(ReconnectionHandler::class.java).`in`(Singleton::class.java)

        // Request external dependencies that should be provided by parent injector
        requireBinding(Vertx::class.java)
        requireBinding(ConnectionStore::class.java)
        requireBinding(ConnectionCache::class.java)
        requireBinding(ChannelStore::class.java)

        // Expose CommandVerticle to the parent injector
        expose(UpSocketVerticle::class.java)
    }

    /**
     * Provides a Router instance specifically for CommandVerticle
     */
    @Provides
    @Singleton
    fun provideRouter(vertx: Vertx): Router {
        return Router.router(vertx)
    }

    /**
     * Provides a CoroutineScope using the Vertx dispatcher
     */
    @Provides
    @Singleton
    fun provideCoroutineScope(coroutineContext: CoroutineContext): CoroutineScope {
        return CoroutineScope(coroutineContext)
    }

    /**
     * Provides EventBusUtils for CommandVerticle
     */
    @Provides
    @Singleton
    fun provideEventBusUtils(vertx: Vertx, coroutineContext: CoroutineContext): EventBusUtils {
        return EventBusUtils(vertx, coroutineContext, "CommandVerticle")
    }

    /**
     * Provides ConnectionTracker for CommandVerticle
     */
    @Provides
    @Singleton
    fun provideConnectionTracker(verticleLogger: VerticleLogger): ConnectionTracker {
        return ConnectionTracker("CommandSocket", verticleLogger)
    }

    /**
     * Provides HeartbeatManager for CommandVerticle
     */
    @Provides
    @Singleton
    fun provideHeartbeatManager(
        vertx: Vertx,
        verticleLogger: VerticleLogger,
        connectionStore: ConnectionStore,
        coroutineScope: CoroutineScope
    ): HeartbeatManager {
        val heartbeatManager = HeartbeatManager(vertx, verticleLogger, connectionStore, coroutineScope)
        heartbeatManager.setHeartbeatInterval(UpSocketVerticle.HEARTBEAT_INTERVAL_MS)
        return heartbeatManager
    }

    /**
     * Provides ConnectionLifecycle for CommandVerticle
     */
    @Provides
    @Singleton
    fun provideConnectionLifecycle(
        vertx: Vertx,
        vlog: VerticleLogger,
        connectionStore: ConnectionStore,
        connectionCache: ConnectionCache,
        channelStore: ChannelStore,
        connectionTracker: ConnectionTracker,
        heartbeatManager: HeartbeatManager
    ): ConnectionLifecycle {
        return ConnectionLifecycle(
            vertx,
            vlog,
            connectionStore,
            connectionCache,
            channelStore,
            connectionTracker,
            heartbeatManager
        )
    }

    @Provides
    @Singleton
    fun provideCommandMessageHandler(
        connectionController: ConnectionController,
        vertx: Vertx,
        connectionCache: ConnectionCache
    ): CommandMessageHandler {
        return CommandMessageHandler(
            connectionController,
            vertx,
            connectionCache
        )
    }
}