package com.minare.worker.downsocket.config

import com.google.inject.PrivateModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.minare.application.config.FrameworkConfig
import com.minare.cache.ConnectionCache
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.storage.interfaces.ContextStore
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.transport.services.HeartbeatManager
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.core.transport.downsocket.DownSocketVerticle
import com.minare.core.transport.downsocket.DownSocketVerticleCache
import com.minare.core.transport.downsocket.events.EntityUpdatedEvent
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import kotlinx.coroutines.CoroutineScope
import com.minare.worker.downsocket.events.UpdateConnectionClosedEvent
import com.minare.worker.downsocket.events.UpdateConnectionEstablishedEvent
import com.minare.worker.downsocket.handlers.EntityUpdateHandler
import kotlin.coroutines.CoroutineContext

/**
 * Specialized Guice module for DownSocketVerticle and its dependencies.
 * This module provides all the necessary components within the DownSocketVerticle's scope.
 */
class DownSocketVerticleModule : PrivateModule() {

    override fun configure() {
        // Bind update verticle itself
        bind(DownSocketVerticle::class.java)

        // Bind all event handlers
        bind(EntityUpdatedEvent::class.java).`in`(Singleton::class.java)
        bind(UpdateConnectionClosedEvent::class.java).`in`(Singleton::class.java)
        bind(UpdateConnectionEstablishedEvent::class.java).`in`(Singleton::class.java)

        // Bind connection handlers
        bind(EntityUpdateHandler::class.java).`in`(Singleton::class.java)

        bind(DownSocketVerticleCache::class.java).`in`(Singleton::class.java)

        // Request external dependencies that should be provided by parent injector
        requireBinding(Vertx::class.java)
        requireBinding(CoroutineContext::class.java)
        requireBinding(CoroutineScope::class.java)
        requireBinding(ConnectionStore::class.java)
        requireBinding(ConnectionCache::class.java)
        requireBinding(ChannelStore::class.java)
        requireBinding(ContextStore::class.java)
        requireBinding(EventBusUtils::class.java)

        // Expose DownSocketVerticle to the parent injector
        expose(DownSocketVerticle::class.java)
    }

    /**
     * Provides a Router instance specifically for DownSocketVerticle
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
     * Provides HeartbeatManager for DownSocketVerticle
     */
    @Provides
    @Singleton
    fun provideHeartbeatManager(
        vertx: Vertx,
        frameworkConfig: FrameworkConfig,
        verticleLogger: VerticleLogger,
        connectionStore: ConnectionStore,
        @Named("verticle-scoped") coroutineScope: CoroutineScope
    ): HeartbeatManager {
        val heartbeatManager = HeartbeatManager(vertx, verticleLogger, connectionStore, coroutineScope)
        heartbeatManager.setHeartbeatInterval(frameworkConfig.sockets.down.heartbeatInterval)
        return heartbeatManager
    }
}