package com.minare.config

import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.name.Names
import com.minare.cache.ConnectionCache
import com.minare.cache.InMemoryConnectionCache
import com.minare.controller.ConnectionController
import com.minare.core.state.MongoEntityStreamConsumer
import com.minare.core.websocket.CommandMessageHandler
import com.minare.core.websocket.CommandSocketManager
import com.minare.core.websocket.UpdateSocketManager
import com.minare.persistence.*
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.dispatcher
import kotlin.coroutines.CoroutineContext

class GuiceModule : AbstractModule() {

    override fun configure() {
        // Store bindings
        bind(EntityStore::class.java).to(MongoEntityStore::class.java)
        bind(ConnectionStore::class.java).to(MongoConnectionStore::class.java)
        bind(ChannelStore::class.java).to(MongoChannelStore::class.java)
        bind(ContextStore::class.java).to(MongoContextStore::class.java)

        // Cache bindings
        bind(ConnectionCache::class.java).to(InMemoryConnectionCache::class.java).`in`(Singleton::class.java)

        bind(String::class.java)
            .annotatedWith(Names.named("mongoDatabase"))
            .toInstance("minare")

        bind(String::class.java)
            .annotatedWith(Names.named("mongoConnectionString"))
            .toInstance("mongodb://localhost:27017")

        bind(String::class.java).annotatedWith(Names.named("channels")).toInstance("channels")
        bind(String::class.java).annotatedWith(Names.named("contexts")).toInstance("contexts")

        bind(MongoEntityStreamConsumer::class.java).asEagerSingleton()
    }

    @Provides
    @Singleton
    fun provideVertx(): Vertx {
        return Vertx.vertx()
    }

    @Provides
    @Singleton
    fun provideCoroutineContext(vertx: Vertx): CoroutineContext {
        return vertx.dispatcher()
    }

    @Provides
    @Singleton
    fun provideMongoClient(vertx: Vertx): MongoClient {
        val uri = System.getenv().getOrDefault("MONGO_URI", "mongodb://mongodb-rs:27017/?replicaSet=rs0")

        val config = JsonObject()
            .put("connection_string", uri)
            .put("db_name", "your_database_name")  // Change this to your actual DB name

        return MongoClient.createShared(vertx, config)
    }

    @Provides
    @Singleton
    fun provideConnectionController(
        connectionStore: ConnectionStore,
        connectionCache: ConnectionCache
    ): ConnectionController {
        return ConnectionController(connectionStore, connectionCache)
    }

    @Provides
    @Singleton
    fun provideCommandMessageHandler(
        connectionController: ConnectionController,
        coroutineContext: CoroutineContext
    ): CommandMessageHandler {
        return CommandMessageHandler(connectionController, coroutineContext)
    }

    @Provides
    @Singleton
    fun provideCommandSocketManager(
        connectionStore: ConnectionStore,
        connectionController: ConnectionController,
        messageHandler: CommandMessageHandler,
        coroutineContext: CoroutineContext
    ): CommandSocketManager {
        return CommandSocketManager(connectionStore, connectionController, messageHandler, coroutineContext)
    }

    @Provides
    @Singleton
    fun provideUpdateSocketManager(
        connectionStore: ConnectionStore,
        connectionController: ConnectionController,
        coroutineContext: CoroutineContext
    ): UpdateSocketManager {
        return UpdateSocketManager(connectionStore, connectionController, coroutineContext)
    }
}