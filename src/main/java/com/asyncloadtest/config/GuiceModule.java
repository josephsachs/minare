// config/GuiceModule.java
package com.asyncloadtest.config;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.asyncloadtest.core.websocket.ConnectionManager;
import com.asyncloadtest.example.ExampleTestServer;
import com.asyncloadtest.persistence.DynamoDBManager;
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
        bind(ExampleTestServer.class).in(Singleton.class);
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

    @Provides
    @Singleton
    DynamoDBManager provideDynamoDBManager(AmazonDynamoDB dynamoDB, Vertx vertx) {
        boolean isLocal = "true".equalsIgnoreCase(System.getenv("DYNAMO_LOCAL"));
        return new DynamoDBManager(isLocal, dynamoDB, vertx);
    }

    @Provides
    @Singleton
    AmazonDynamoDB provideDynamoDB() {
        boolean isLocal = "true".equalsIgnoreCase(System.getenv("DYNAMO_LOCAL"));
        String endpoint = System.getenv().getOrDefault("DYNAMO_ENDPOINT", "http://localhost:8000");
        String region = System.getenv().getOrDefault("AWS_REGION", "us-west-2");

        if (isLocal) {
            return AmazonDynamoDBClientBuilder.standard()
                    .withEndpointConfiguration(
                            new AwsClientBuilder.EndpointConfiguration(endpoint, region)
                    )
                    .withCredentials(new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials("dummy", "dummy")))
                    .build();
        } else {
            return AmazonDynamoDBClientBuilder.defaultClient();
        }
    }
}