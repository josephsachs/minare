package com.minare.core.transport.upsocket.config

import com.google.inject.PrivateModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.minare.application.config.FrameworkConfig
import com.minare.controller.ConnectionController
import com.minare.controller.MessageController
import com.minare.controller.OperationController
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.adapters.WebsocketProtocol
import com.minare.core.transport.interfaces.SocketProtocol
import com.minare.core.transport.upsocket.UpSocketVerticle
import com.minare.core.transport.upsocket.events.EntitySyncEvent
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.worker.upsocket.events.ConnectionCleanupEvent
import io.vertx.core.Vertx
import kotlinx.coroutines.CoroutineScope
import kotlin.coroutines.CoroutineContext

class UpSocketVerticleModule : PrivateModule() {

    override fun configure() {
        bind(UpSocketVerticle::class.java)

        bind(SocketProtocol::class.java).to(WebsocketProtocol::class.java).`in`(Singleton::class.java)

        bind(EntitySyncEvent::class.java).`in`(Singleton::class.java)
        bind(ConnectionCleanupEvent::class.java).`in`(Singleton::class.java)

        requireBinding(Vertx::class.java)
        requireBinding(CoroutineContext::class.java)
        requireBinding(CoroutineScope::class.java)
        requireBinding(ConnectionStore::class.java)
        requireBinding(ChannelStore::class.java)
        requireBinding(ConnectionController::class.java)
        requireBinding(OperationController::class.java)
        requireBinding(MessageController::class.java)
        requireBinding(EventBusUtils::class.java)

        expose(UpSocketVerticle::class.java)
    }

    @Provides
    @Singleton
    @Named("verticle-scoped")
    fun provideCoroutineScope(coroutineContext: CoroutineContext): CoroutineScope {
        return CoroutineScope(coroutineContext)
    }
}