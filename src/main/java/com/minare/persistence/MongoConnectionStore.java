package com.minare.persistence;

import io.vertx.ext.mongo.MongoClient;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;

@Slf4j
@Singleton
public class MongoConnectionStore implements ConnectionStore {
    private final MongoClient mongoClient;
    private static final String COLLECTION_NAME = "connections";

    @Inject
    public MongoConnectionStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public Future<Void> storeConnection(String connectionId, long timestamp) {
        JsonObject connection = new JsonObject()
                .put("_id", connectionId)
                .put("timestamp", timestamp);

        return mongoClient.insert(COLLECTION_NAME, connection)
                .onSuccess(result -> log.info("Stored connection {}", connectionId))
                .mapEmpty();
    }

    @Override
    public Future<Void> removeConnection(String connectionId) {
        JsonObject query = new JsonObject()
                .put("_id", connectionId);

        return mongoClient.removeDocument(COLLECTION_NAME, query)
                .onSuccess(result -> log.info("Removed connection {}", connectionId))
                .mapEmpty();
    }

    @Override
    public Future<Boolean> isConnectionActive(String connectionId) {
        JsonObject query = new JsonObject()
                .put("_id", connectionId);

        return mongoClient.findOne(COLLECTION_NAME, query, null)
                .map(result -> result != null);
    }
}