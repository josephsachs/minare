package com.minare.config

import com.google.inject.AbstractModule
import com.google.inject.Injector
import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.name.Names
import com.minare.cache.ConnectionCache
import com.minare.cache.InMemoryConnectionCache
import com.minare.controller.ConnectionController
import com.minare.core.entity.ReflectionCache
import com.minare.worker.ChangeStreamWorkerVerticle
import com.minare.worker.CleanupVerticle
import com.minare.worker.MutationVerticle
import com.minare.core.websocket.CommandMessageHandler
import com.minare.core.websocket.CommandSocketManager
import com.minare.core.websocket.UpdateSocketManager
import com.minare.worker.MinareVerticleFactory
import com.minare.persistence.*
import com.minare.worker.CommandSocketVerticle
import io.vertx.core.Vertx
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.dispatcher
import javax.inject.Named
import kotlin.coroutines.CoroutineContext

/**
 * Core framework Guice module that provides default bindings.
 * Applications can override these bindings by using a child injector.
 */
class MinareModule : AbstractModule() {
    private val log = LoggerFactory.getLogger(MinareModule::class.java)

    override fun configure() {
        // Store bindings
        bind(EntityStore::class.java).to(MongoEntityStore::class.java)
        bind(ConnectionStore::class.java).to(MongoConnectionStore::class.java)
        bind(ChannelStore::class.java).to(MongoChannelStore::class.java)
        bind(ContextStore::class.java).to(MongoContextStore::class.java)
        bind(ConnectionCache::class.java).to(InMemoryConnectionCache::class.java).`in`(Singleton::class.java)

        // Core configuration
        bind(String::class.java)
            .annotatedWith(Names.named("mongoConnectionString"))
            .toInstance("mongodb://localhost:27017")

        // Collection names
        bind(String::class.java).annotatedWith(Names.named("channels")).toInstance("channels")
        bind(String::class.java).annotatedWith(Names.named("contexts")).toInstance("contexts")

        // Register the verticles
        bind(ChangeStreamWorkerVerticle::class.java)
        bind(MutationVerticle::class.java)
    }

    /**
     * Provides the MinareVerticleFactory
     */
    @Provides
    @Singleton
    fun provideMinareVerticleFactory(injector: Injector): MinareVerticleFactory {
        return MinareVerticleFactory(injector)
    }

    @Provides
    @Singleton
    fun provideCoroutineContext(vertx: Vertx): CoroutineContext {
        return vertx.dispatcher()
    }

    @Provides
    @Singleton
    fun provideMongoClient(vertx: Vertx, @Named("databaseName") dbName: String): MongoClient {
        // First try environment variable, fallback to localhost if not available
        val uri = System.getenv("MONGO_URI") ?: "mongodb://localhost:27017"

        log.info("Connecting to MongoDB at: $uri with database: $dbName")

        val config = JsonObject()
            .put("connection_string", uri)
            .put("db_name", dbName)
            .put("useObjectId", true)
            // Set write concern for replica set environment
            .put("writeConcern", "majority")
            // Add timeout settings
            .put("serverSelectionTimeoutMS", 5000)
            .put("connectTimeoutMS", 10000)
            .put("socketTimeoutMS", 60000)

        return MongoClient.createShared(vertx, config)
    }

    @Provides
    @Singleton
    fun provideCommandMessageHandler(
        connectionController: ConnectionController,
        coroutineContext: CoroutineContext,
        entityStore: EntityStore,
        reflectionCache: ReflectionCache,
        vertx: Vertx,
        connectionCache: ConnectionCache
    ): CommandMessageHandler {
        return CommandMessageHandler(
            connectionController,
            coroutineContext,
            entityStore,
            reflectionCache,
            vertx,
            connectionCache
        )
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
        coroutineContext: CoroutineContext,
        connectionCache: ConnectionCache,
        vertx: Vertx
    ): UpdateSocketManager {
        return UpdateSocketManager(
            connectionStore,
            connectionController,
            coroutineContext,
            connectionCache,
            vertx
        )
    }

    @Provides
    @Singleton
    fun provideCleanupVerticle(
        connectionStore: ConnectionStore,
        connectionCache: ConnectionCache,
        connectionController: ConnectionController
    ): CleanupVerticle {
        return CleanupVerticle(
            connectionStore,
            connectionCache,
            connectionController
        )
    }

    @Provides
    @Singleton
    fun provideCommandSocketVerticle(
        connectionStore: ConnectionStore,
        connectionCache: ConnectionCache,
        connectionController: ConnectionController,
        channelStore: ChannelStore,
        contextStore: ContextStore,
        messageHandler: CommandMessageHandler,
        reflectionCache: ReflectionCache,
        entityStore: EntityStore
    ): CommandSocketVerticle {
        return CommandSocketVerticle(
            connectionStore,
            connectionCache,
            connectionController,
            channelStore,
            contextStore,
            messageHandler,
            reflectionCache,
            entityStore
        )
    }
}