package com.minare.persistence;

import io.vertx.core.Future;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.mongo.IndexOptions;
import io.vertx.core.json.JsonObject;
import java.util.concurrent.TimeUnit;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;

@Slf4j
@Singleton
public class DatabaseInitializer {
    private final MongoClient mongoClient;
    private static final String DB_NAME = "minare";

    @Inject
    public DatabaseInitializer(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public Future<Void> initialize() {
        if ("true".equalsIgnoreCase(System.getenv("RESET_DB"))) {
            log.info("RESET_DB=true, dropping collections");
            return mongoClient.dropCollection("connections")
                    .compose(v -> mongoClient.dropCollection("entities"))
                    .compose(v -> mongoClient.dropCollection("contexts"))
                    .compose(v -> initializeAllCollections());
        }
        return initializeAllCollections();
    }

    private Future<Void> initializeAllCollections() {
        return initializeConnections()
                .compose(v -> initializeEntities())
                .compose(v -> initializeContexts());
    }

    private Future<Void> initializeConnections() {
        JsonObject index = new JsonObject()
                .put("timestamp", 1);

        IndexOptions indexOptions = new IndexOptions()
                .name("timestamp_ttl_idx")
                .expireAfter(60L, TimeUnit.SECONDS);

        return mongoClient.createCollection("connections")
                .compose(v -> mongoClient.createIndexWithOptions("connections", index, indexOptions))
                .onSuccess(v -> log.info("Initialized connections collection with TTL index"))
                .mapEmpty();
    }

    private Future<Void> initializeEntities() {
        JsonObject index = new JsonObject()
                .put("type", 1);

        return mongoClient.createCollection("entities")
                .compose(v -> mongoClient.createIndex("entities", index))
                .onSuccess(v -> log.info("Initialized entities collection with type index"))
                .mapEmpty();
    }

    private Future<Void> initializeContexts() {
        JsonObject index = new JsonObject()
                .put("channelId", 1);

        return mongoClient.createCollection("contexts")
                .compose(v -> mongoClient.createIndex("contexts", index))
                .onSuccess(v -> log.info("Initialized contexts collection with channel index"))
                .mapEmpty();
    }
}