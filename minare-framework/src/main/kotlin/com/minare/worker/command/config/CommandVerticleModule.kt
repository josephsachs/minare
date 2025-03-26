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

class CommandVerticleModule : PrivateModule() {
    override fun configure() {
        // Bind all the handlers
        bind(EntitySyncEvent::class.java).`in`(Singleton::class.java)
        bind(ConnectionCleanupEvent::class.java).`in`(Singleton::class.java)
        bind(ChannelCleanupEvent::class.java).`in`(Singleton::class.java)
        bind(CommandSocketCleanupEvent::class.java).`in`(Singleton::class.java)
        bind(CommandSocketInitEvent::class.java).`in`(Singleton::class.java)

        bind(CloseHandler::class.java).`in`(Singleton::class.java)
        bind(MessageHandler::class.java).`in`(Singleton::class.java)
        bind(ReconnectionHandler::class.java).`in`(Singleton::class.java)

        expose(CommandVerticle::class.java)
    }

    @Provides
    @Singleton
    fun provideRouter(): Router {
        return Router.router(Vertx.vertx())
    }

    @Provides
    @Singleton
    fun provideVerticleLogger(): VerticleLogger {
        return VerticleLogger()
    }

    @Provides
    @Singleton
    fun provideCoroutineContext(): CoroutineContext {
        return Vertx.vertx().dispatcher()
    }

    @Provides
    @Singleton
    fun provideCoroutineScope(coroutineContext: CoroutineContext): CoroutineScope {
        return CoroutineScope(coroutineContext)
    }

    @Provides
    @Singleton
    fun provideEventBusUtils(coroutineContext: CoroutineContext): EventBusUtils {
        return provideVerticleLogger().createEventBusUtils()
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