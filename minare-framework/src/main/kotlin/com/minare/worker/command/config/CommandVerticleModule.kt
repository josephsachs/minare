package com.minare.worker.command.config

import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.minare.cache.ConnectionCache
import com.minare.persistence.ChannelStore
import com.minare.persistence.ConnectionStore
import com.minare.utils.ConnectionTracker
import com.minare.utils.EventBusUtils
import com.minare.utils.HeartbeatManager
import com.minare.utils.VerticleLogger
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import com.minare.worker.command.CommandVerticle
import com.minare.worker.command.ConnectionLifecycle
import com.minare.worker.command.events.EntitySyncEvent
import com.minare.worker.command.events.ConnectionCleanupEvent
import com.minare.worker.command.events.ChannelCleanupEvent
import com.minare.worker.command.events.CommandSocketCleanupEvent
import com.minare.worker.command.events.CommandSocketInitEvent
import com.minare.worker.command.handlers.CloseHandler
import com.minare.worker.command.handlers.MessageHandler
import com.minare.worker.command.handlers.ReconnectionHandler
import kotlin.coroutines.CoroutineContext

/**
 * Specialized Guice module for CommandVerticle and its dependencies.
 * This module provides all the necessary components within the CommandVerticle's scope.
 */
class CommandVerticleModule : AbstractModule() {

    override fun configure() {
        bind(EntitySyncEvent::class.java).`in`(Singleton::class.java)
        bind(ConnectionCleanupEvent::class.java).`in`(Singleton::class.java)
        bind(ChannelCleanupEvent::class.java).`in`(Singleton::class.java)
        bind(CommandSocketCleanupEvent::class.java).`in`(Singleton::class.java)
        bind(CommandSocketInitEvent::class.java).`in`(Singleton::class.java)

        bind(CloseHandler::class.java).`in`(Singleton::class.java)
        bind(MessageHandler::class.java).`in`(Singleton::class.java)
        bind(ReconnectionHandler::class.java).`in`(Singleton::class.java)
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
     * Provides a CoroutineContext using the Vertx dispatcher
     */
    @Provides
    @Singleton
    fun provideCoroutineContext(vertx: Vertx): CoroutineContext {
        return vertx.dispatcher()
    }

    /**
     * Provides a CoroutineScope using the Vertx dispatcher
     */
    @Provides
    @Singleton
    fun provideCoroutineScope(vertx: Vertx): CoroutineScope {
        return CoroutineScope(vertx.dispatcher())
    }

    /**
     * Provides a properly initialized VerticleLogger for CommandVerticle
     * Note: We can't initialize it with the actual verticle instance yet
     * because the verticle hasn't been created. We'll need to initialize
     * it within the verticle's start method.
     */
    @Provides
    @Singleton
    fun provideVerticleLogger(context: CommandVerticle): VerticleLogger {
        return VerticleLogger(context)
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
        heartbeatManager.setHeartbeatInterval(CommandVerticle.HEARTBEAT_INTERVAL_MS)
        return heartbeatManager
    }

    /**
     * Provides ConnectionLifecycle for CommandVerticle
     */
    @Provides
    @Singleton
    fun provideConnectionLifecycle(
        vlog: VerticleLogger,
        connectionStore: ConnectionStore,
        connectionCache: ConnectionCache,
        channelStore: ChannelStore,
        connectionTracker: ConnectionTracker,
        heartbeatManager: HeartbeatManager
    ): ConnectionLifecycle {
        return ConnectionLifecycle(
            vlog,
            connectionStore,
            connectionCache,
            channelStore,
            connectionTracker,
            heartbeatManager
        )
    }
}