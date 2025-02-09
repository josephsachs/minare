// config/GuiceModule.java
package com.asyncloadtest.config;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.asyncloadtest.core.websocket.ConnectionManager;
import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.config.Config;
import com.hazelcast.core.Hazelcast;
import com.asyncloadtest.controller.AbstractEntityController;
import com.asyncloadtest.example.ExampleEntityController;
import com.asyncloadtest.core.websocket.WebSocketManager;
import com.asyncloadtest.core.state.EntityStateManager;
import io.vertx.core.Vertx;

public class GuiceModule extends AbstractModule {

    @Override
    protected void configure() {
        // Bind our example implementation
        bind(AbstractEntityController.class).to(ExampleEntityController.class);
    }

    @Provides
    @Singleton
    Vertx provideVertx() {
        return Vertx.vertx();
    }

    @Provides
    @Singleton
    HazelcastInstance provideHazelcast() {
        Config config = new Config();
        config.setClusterName("asyncloadtest");
        // Configure maps with TTL
        config.getMapConfig("checksums")
                .setTimeToLiveSeconds(12);
        return Hazelcast.newHazelcastInstance(config);
    }

    @Provides
    @Singleton
    WebSocketManager provideWebSocketManager(
            AbstractEntityController controller,
            EntityStateManager stateManager,
            ConnectionManager connectionManager
    ) {
        return new WebSocketManager(controller, stateManager, connectionManager);
    }

    // config/GuiceModule.java
    @Provides
    @Singleton
    ConnectionManager provideConnectionManager(AmazonDynamoDB dynamoDB, HazelcastInstance hazelcast) {
        return new ConnectionManager(dynamoDB, hazelcast);
    }
}