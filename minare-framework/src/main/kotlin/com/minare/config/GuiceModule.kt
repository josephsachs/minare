package com.minare.config

import com.google.inject.AbstractModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.minare.core.state.MongoChangeStreamConsumer
import com.minare.core.websocket.CommandMessageHandler
import com.minare.core.websocket.CommandSocketManager
import com.minare.core.websocket.ConnectionManager
import com.minare.core.websocket.UpdateSocketManager
import com.minare.persistence.*
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient

class GuiceModule : AbstractModule() {

    override fun configure() {
        // Store bindings
        bind(EntityStore::class.java) to MongoEntityStore::class.java
        bind(ConnectionStore::class.java) to MongoConnectionStore::class.java
    }

    @Provides
    @Singleton
    fun provideVertx(): Vertx {
        return Vertx.vertx()
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
    fun provideConnectionManager(): ConnectionManager {
        return ConnectionManager()
    }

    @Provides
    @Singleton
    fun provideCommandMessageHandler(): CommandMessageHandler {
        return CommandMessageHandler()
    }

    @Provides
    @Singleton
    fun provideCommandSocketManager(
        connectionStore: ConnectionStore,
        connectionManager: ConnectionManager,
        messageHandler: CommandMessageHandler
    ): CommandSocketManager {
        return CommandSocketManager(connectionStore, connectionManager, messageHandler)
    }

    @Provides
    @Singleton
    fun provideUpdateSocketManager(
        connectionStore: ConnectionStore,
        connectionManager: ConnectionManager
    ): UpdateSocketManager {
        return UpdateSocketManager(connectionStore, connectionManager)
    }

    @Provides
    @Singleton
    fun provideMongoChangeStreamConsumer(
        mongoClient: MongoClient,
        updateSocketManager: UpdateSocketManager,
        connectionManager: ConnectionManager,
        vertx: Vertx
    ): MongoChangeStreamConsumer {
        return MongoChangeStreamConsumer(mongoClient, updateSocketManager, connectionManager, vertx)
    }
}