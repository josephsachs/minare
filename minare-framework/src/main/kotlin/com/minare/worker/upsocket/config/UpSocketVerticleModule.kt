package com.minare.worker.upsocket.config

import com.google.inject.PrivateModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.minare.cache.ConnectionCache
import com.minare.controller.OperationController
import com.minare.persistence.ChannelStore
import com.minare.persistence.ConnectionStore
import com.minare.utils.ConnectionTracker
import com.minare.utils.EventBusUtils
import com.minare.utils.HeartbeatManager
import com.minare.utils.VerticleLogger
import com.minare.worker.upsocket.SyncCommandHandler
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
 *
 * Updated for Kafka architecture:
 * - CommandMessageHandler moved to main module (used by MessageQueueConsumerVerticle)
 * - Added SyncCommandHandler for sync operations
 * - Added OperationController requirement
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
        bind(MessageHandler::class.java).`in`(Singleton::class.java)
        bind(ReconnectionHandler::class.java).`in`(Singleton::class.java)

        // Sync handler (temporary, outside Kafka flow)
        bind(SyncCommandHandler::class.java).`in`(Singleton::class.java)

        // Request external dependencies that should be provided by parent injector
        requireBinding(Vertx::class.java)
        requireBinding(ConnectionStore::class.java)
        requireBinding(ConnectionCache::class.java)
        requireBinding(ChannelStore::class.java)
        requireBinding(OperationController::class.java)

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

    /**
     * Provides a CoroutineScope using the Vertx dispatcher
     */
    @Provides
    @Singleton
    fun provideCoroutineScope(coroutineContext: CoroutineContext): CoroutineScope {
        return CoroutineScope(coroutineContext)
    }

    /**
     * Provides EventBusUtils for UpSocketVerticle
     */
    @Provides
    @Singleton
    fun provideEventBusUtils(vertx: Vertx, coroutineContext: CoroutineContext): EventBusUtils {
        return EventBusUtils(vertx, coroutineContext, "UpSocketVerticle")
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
        connectionStore: ConnectionStore,
        coroutineScope: CoroutineScope
    ): HeartbeatManager {
        val heartbeatManager = HeartbeatManager(vertx, verticleLogger, connectionStore, coroutineScope)
        heartbeatManager.setHeartbeatInterval(UpSocketVerticle.HEARTBEAT_INTERVAL_MS)
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