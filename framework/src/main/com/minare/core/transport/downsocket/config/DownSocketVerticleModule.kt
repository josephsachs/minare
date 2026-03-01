package com.minare.core.transport.downsocket.config

import com.google.inject.PrivateModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.minare.application.config.FrameworkConfig
import com.minare.core.storage.adapters.MongoEntityStore
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.storage.interfaces.ContextStore
import com.minare.core.storage.interfaces.EntityGraphStore
import com.minare.core.transport.adapters.WebsocketProtocol
import com.minare.core.transport.downsocket.DownSocketVerticle
import com.minare.core.transport.downsocket.DownSocketVerticleCache
import com.minare.core.transport.downsocket.events.EntityUpdatedEvent
import com.minare.core.transport.downsocket.handlers.EntityUpdateHandler
import com.minare.core.transport.interfaces.SocketProtocol
import com.minare.core.transport.models.SocketTypeConfigOption
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.exceptions.ConfigurationException
import com.minare.worker.downsocket.events.UpdateConnectionClosedEvent
import com.minare.worker.downsocket.events.UpdateConnectionEstablishedEvent
import io.vertx.core.Vertx
import kotlinx.coroutines.CoroutineScope
import org.slf4j.LoggerFactory
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext

class DownSocketVerticleModule(
    private val frameworkConfig: FrameworkConfig
) : PrivateModule() {
    private val log = LoggerFactory.getLogger(DownSocketVerticleModule::class.java)

    override fun configure() {
        bind(DownSocketVerticle::class.java)

        when (frameworkConfig.sockets.down.type) {
            SocketTypeConfigOption.WEBSOCKET -> {
                bind(SocketProtocol::class.java).to(WebsocketProtocol::class.java)
            }
            else -> {
                throw ConfigurationException("No socket type configured for down")
            }
        }

        bind(EntityUpdatedEvent::class.java).`in`(Singleton::class.java)
        bind(UpdateConnectionClosedEvent::class.java).`in`(Singleton::class.java)
        bind(UpdateConnectionEstablishedEvent::class.java).`in`(Singleton::class.java)
        bind(EntityUpdateHandler::class.java).`in`(Singleton::class.java)
        bind(DownSocketVerticleCache::class.java).`in`(Singleton::class.java)

        requireBinding(Vertx::class.java)
        requireBinding(CoroutineContext::class.java)
        requireBinding(CoroutineScope::class.java)
        requireBinding(ConnectionStore::class.java)
        requireBinding(ChannelStore::class.java)
        requireBinding(ContextStore::class.java)
        requireBinding(EventBusUtils::class.java)

        expose(DownSocketVerticle::class.java)
    }

    @Provides
    @Singleton
    @Named("verticle-scoped")
    fun provideCoroutineScope(coroutineContext: CoroutineContext): CoroutineScope {
        return CoroutineScope(coroutineContext)
    }
}