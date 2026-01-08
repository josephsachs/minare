package com.minare.worker.upsocket.config

import com.google.inject.PrivateModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.minare.application.config.FrameworkConfig
import com.minare.cache.ConnectionCache
import com.minare.controller.MessageController
import com.minare.controller.OperationController
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.downsocket.services.ConnectionTracker
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.utils.HeartbeatManager
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.transport.upsocket.handlers.SyncCommandHandler
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import kotlinx.coroutines.CoroutineScope
import com.minare.worker.upsocket.UpSocketVerticle
import com.minare.worker.upsocket.ConnectionLifecycle
import com.minare.worker.upsocket.events.*
import com.minare.worker.upsocket.handlers.CloseHandler
import com.minare.worker.upsocket.handlers.ReconnectionHandler
import io.vertx.kotlin.coroutines.dispatcher
import kotlin.coroutines.CoroutineContext

/**
 * Specialized Guice module for UpSocketVerticle and its dependencies.
 * This module provides all the necessary components within the UpSocketVerticle's scope.
 */
class UpSocketVerticleModule : PrivateModule() {

    override fun configure() {
        bind(UpSocketVerticle::class.java)

        // Event handlers
        bind(EntitySyncEvent::class.java).`in`(Singleton::class.java)
        bind(ConnectionCleanupEvent::class.java).`in`(Singleton::class.java)
        bind(ChannelCleanupEvent::class.java).`in`(Singleton::class.java)
        bind(UpSocketCleanupEvent::class.java).`in`(Singleton::class.java)
        bind(UpSocketInitEvent::class.java).`in`(Singleton::class.java)
        bind(UpSocketGetRouterEvent::class.java).`in`(Singleton::class.java)

        // Message handlers
        bind(CloseHandler::class.java).`in`(Singleton::class.java)
        bind(ReconnectionHandler::class.java).`in`(Singleton::class.java)

        // Request external dependencies that should be provided by parent injector
        requireBinding(Vertx::class.java)
        requireBinding(CoroutineContext::class.java)
        requireBinding(CoroutineScope::class.java)
        requireBinding(ConnectionStore::class.java)
        requireBinding(ConnectionCache::class.java)
        requireBinding(ChannelStore::class.java)
        requireBinding(OperationController::class.java)
        requireBinding(MessageController::class.java)
        requireBinding(EventBusUtils::class.java)

        // Expose UpSocketVerticle to the parent injector
        expose(UpSocketVerticle::class.java)
    }

    /**
     * Provides a Router instance specifically for UpSocketVerticle
     */
    @Provides
    @Singleton
    fun provideRouter(vertx: Vertx): Router {
        return Router.router(vertx)
    }

    @Provides
    @Singleton
    @Named("verticle-scoped")
    fun provideCoroutineScope(coroutineContext: CoroutineContext): CoroutineScope {
        return CoroutineScope(coroutineContext)
    }

    /**
     * Provides ConnectionTracker for UpSocketVerticle
     */
    @Provides
    @Singleton
    fun provideConnectionTracker(verticleLogger: VerticleLogger): ConnectionTracker {
        return ConnectionTracker("UpSocket", verticleLogger)
    }

    /**
     * Provides HeartbeatManager for UpSocketVerticle
     */
    @Provides
    @Singleton
    fun provideHeartbeatManager(
        vertx: Vertx,
        verticleLogger: VerticleLogger,
        frameworkConfig: FrameworkConfig,
        connectionStore: ConnectionStore,
        @Named("verticle-scoped") coroutineScope: CoroutineScope
    ): HeartbeatManager {
        val heartbeatManager = HeartbeatManager(vertx, verticleLogger, connectionStore, coroutineScope)
        heartbeatManager.setHeartbeatInterval(frameworkConfig.sockets.up.heartbeatInterval)
        return heartbeatManager
    }

    /**
     * Provides ConnectionLifecycle for UpSocketVerticle
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
}