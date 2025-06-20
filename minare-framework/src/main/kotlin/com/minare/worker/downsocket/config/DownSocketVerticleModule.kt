package com.minare.worker.downsocket.config

import com.google.inject.PrivateModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.minare.cache.ConnectionCache
import com.minare.persistence.ChannelStore
import com.minare.persistence.ConnectionStore
import com.minare.persistence.ContextStore
import com.minare.utils.EventBusUtils
import com.minare.utils.HeartbeatManager
import com.minare.utils.VerticleLogger
import com.minare.worker.downsocket.DownSocketVerticle
import com.minare.worker.downsocket.DownSocketVerticleCache
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import kotlinx.coroutines.CoroutineScope
import com.minare.worker.downsocket.events.EntityUpdatedEvent
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
        requireBinding(ConnectionStore::class.java)
        requireBinding(ConnectionCache::class.java)
        requireBinding(ChannelStore::class.java)
        requireBinding(ContextStore::class.java)

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

    /**
     * Provides a CoroutineScope using the Vertx dispatcher
     */
    @Provides
    @Singleton
    fun provideCoroutineScope(coroutineContext: CoroutineContext): CoroutineScope {
        return CoroutineScope(coroutineContext)
    }

    /**
     * Provides EventBusUtils for DownSocketVerticle
     */
    @Provides
    @Singleton
    fun provideEventBusUtils(vertx: Vertx, coroutineContext: CoroutineContext): EventBusUtils {
        return EventBusUtils(vertx, coroutineContext, "DownSocketVerticle")
    }

    /**
     * Provides HeartbeatManager for DownSocketVerticle
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
        heartbeatManager.setHeartbeatInterval(DownSocketVerticle.HEARTBEAT_INTERVAL_MS)
        return heartbeatManager
    }
}