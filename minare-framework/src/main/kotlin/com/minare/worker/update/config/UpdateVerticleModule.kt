package com.minare.worker.command.config

import com.google.inject.PrivateModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.minare.cache.ConnectionCache
import com.minare.persistence.ChannelStore
import com.minare.persistence.ConnectionStore
import com.minare.utils.ConnectionTracker
import com.minare.utils.EventBusUtils
import com.minare.utils.HeartbeatManager
import com.minare.utils.VerticleLogger
import com.minare.worker.update.UpdateVerticle
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import kotlinx.coroutines.CoroutineScope
import com.minare.worker.command.ConnectionLifecycle
import com.minare.worker.update.UpdateVerticleCache
import com.minare.worker.update.events.EntityUpdatedEvent
import com.minare.worker.update.events.UpdateConnectionClosedEvent
import com.minare.worker.update.events.UpdateConnectionEstablishedEvent
import com.minare.worker.update.handlers.EntityUpdateHandler
import kotlin.coroutines.CoroutineContext

/**
 * Specialized Guice module for CommandVerticle and its dependencies.
 * This module provides all the necessary components within the UpdateVerticle's scope.
 */
class UpdateVerticleModule : PrivateModule() {

    override fun configure() {
        // Bind command verticle itself
        bind(UpdateVerticle::class.java)

        // Bind all event handlers
        bind(EntityUpdatedEvent::class.java).`in`(Singleton::class.java)
        bind(UpdateConnectionClosedEvent::class.java).`in`(Singleton::class.java)
        bind(UpdateConnectionEstablishedEvent::class.java).`in`(Singleton::class.java)

        // Bind connection handlers
        bind(EntityUpdateHandler::class.java).`in`(Singleton::class.java)

        // Request external dependencies that should be provided by parent injector
        requireBinding(Vertx::class.java)
        requireBinding(ConnectionStore::class.java)
        requireBinding(ConnectionCache::class.java)
        requireBinding(ChannelStore::class.java)

        // Expose CommandVerticle to the parent injector
        expose(UpdateVerticle::class.java)
    }

    /**
     * Provides a Router instance specifically for UpdateVerticle
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
     * Provides EventBusUtils for UpdateVerticle
     */
    @Provides
    @Singleton
    fun provideEventBusUtils(vertx: Vertx, coroutineContext: CoroutineContext): EventBusUtils {
        return EventBusUtils(vertx, coroutineContext, "CommandVerticle")
    }

    /**
     * Provides HeartbeatManager for UpdateVerticle
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
        heartbeatManager.setHeartbeatInterval(UpdateVerticle.HEARTBEAT_INTERVAL_MS)
        return heartbeatManager
    }
}