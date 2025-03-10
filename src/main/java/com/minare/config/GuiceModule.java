package com.minare.config;

import com.minare.core.websocket.ConnectionManager;
import com.minare.example.ExampleTestServer;
import com.minare.persistence.*;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.minare.controller.EntityController;
import com.minare.example.ExampleEntityController;
import com.minare.core.websocket.WebSocketManager;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;

public class GuiceModule extends AbstractModule {
    @Override
    protected void configure() {
        // Store bindings
        //bind(MongoConnectionStore.class).to(MongoConnectionStore.class);
        bind(EntityStore.class).to(MongoEntityStore.class);
        bind(ContextStore.class).to(MongoContextStore.class);
        bind(UserStore.class).to(MongoUserStore.class);
        bind(ConnectionStore.class).to(MongoConnectionStore.class);

        // Existing bindings
        bind(EntityController.class).to(ExampleEntityController.class);
        bind(ExampleTestServer.class).in(Singleton.class);
    }

    @Provides
    @Singleton
    Vertx provideVertx() {
        return Vertx.vertx();
    }

    @Provides
    @Singleton
    MongoClient provideMongoClient(Vertx vertx) {
        String uri = System.getenv().getOrDefault("MONGO_URI", "mongodb://mongodb-rs:27017/?replicaSet=rs0");

        JsonObject config = new JsonObject()
                .put("connection_string", uri)
                .put("db_name", "your_database_name");  // Change this to your actual DB name

        return MongoClient.createShared(vertx, config);
    }

    @Provides
    @Singleton
    WebSocketManager provideWebSocketManager(
            ConnectionStore connectionStore,
            ConnectionManager connectionManager,
            UserStore userStore) {
        return new WebSocketManager(connectionStore, userStore, connectionManager);
    }

    @Provides
    @Singleton
    ConnectionManager provideConnectionManager(MongoConnectionStore connectionStore) {
        return new ConnectionManager(Vertx.vertx());
    }
}
