// config/GuiceModule.java
package com.asyncloadtest.config;

import com.asyncloadtest.core.websocket.ConnectionManager;
import com.asyncloadtest.example.ExampleTestServer;
import com.asyncloadtest.persistence.*;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.asyncloadtest.controller.AbstractEntityController;
import com.asyncloadtest.example.ExampleEntityController;
import com.asyncloadtest.core.websocket.WebSocketManager;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import io.vertx.core.Vertx;

public class GuiceModule extends AbstractModule {
    @Override
    protected void configure() {
        // Store bindings
        bind(ConnectionStore.class).to(MongoConnectionStore.class);
        bind(EntityStore.class).to(MongoEntityStore.class);
        bind(ContextStore.class).to(MongoContextStore.class);

        // Existing bindings
        bind(AbstractEntityController.class).to(ExampleEntityController.class);
        bind(ExampleTestServer.class).in(Singleton.class);
    }

    @Provides
    @Singleton
    MongoClient provideMongoClient() {
        String uri = System.getenv().getOrDefault("MONGO_URI",
                "mongodb://mongodb-rs:27017/?replicaSet=rs0");
        try {
            return MongoClients.create(uri);
        } catch (Exception e) {
            //log.error("Failed to create MongoDB client", e);
            throw e;
        }
    }

    @Provides
    @Singleton
    Vertx provideVertx() {
        return Vertx.vertx();
    }

    @Provides
    @Singleton
    WebSocketManager provideWebSocketManager(
            AbstractEntityController controller,
            ConnectionManager connectionManager,
            ContextStore contextStore) {  // Removed DynamoDB dependency
        return new WebSocketManager(controller, connectionManager, contextStore);
    }

    @Provides
    @Singleton
    ConnectionManager provideConnectionManager(ConnectionStore connectionStore) {
        return new ConnectionManager(connectionStore);
    }
}