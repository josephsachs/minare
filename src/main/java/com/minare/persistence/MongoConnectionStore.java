package com.minare.persistence;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minare.core.models.Connection;
import io.vertx.core.Promise;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Objects;
import java.util.UUID;

@Slf4j
@Singleton
public class MongoConnectionStore implements ConnectionStore {
    private final MongoClient mongoClient;
    private static final String COLLECTION_NAME = "connections";

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Inject
    public MongoConnectionStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public Future<Connection> create(String userId) {
        JsonObject document = new JsonObject()
                .put("userId", userId)
                .put("timestamp", System.currentTimeMillis());

        return mongoClient.insert(COLLECTION_NAME, document)
                .compose(id -> mongoClient.findOne(COLLECTION_NAME, new JsonObject().put("_id", id), null))
                .map(this::deserialize);
    }


    @Override
    public Future<Connection> update(String userId, String connectionId) {
        var timestamp = System.currentTimeMillis();

        JsonObject query = new JsonObject().put("_id", connectionId);
        JsonObject update = new JsonObject().put("$set", new JsonObject()
                .put("userId", userId)
                .put("timestamp", timestamp));

        return mongoClient.updateCollection(COLLECTION_NAME, query, update)
                .compose(result -> {
                    if (result.getDocModified() > 0) {
                        log.info("Updated connection {}", connectionId);
                        // Fetch the updated document and return it
                        return mongoClient.findOne(COLLECTION_NAME, query, null)
                                .map(this::deserialize);
                    } else {
                        log.warn("No connection found to update for id {}", connectionId);
                        return Future.failedFuture("Connection not found");
                    }
                });
    }

    @Override
    public Future<Void> delete(String connectionId) {
        JsonObject query = new JsonObject()
                .put("_id", connectionId);

        return mongoClient.removeDocument(COLLECTION_NAME, query)
                .onSuccess(result -> log.info("Removed connection {}", connectionId))
                .mapEmpty();
    }

    //@Override
    public Future<Connection> find(String connectionId) {
        return mongoClient.findOne(COLLECTION_NAME, new JsonObject().put("_id", connectionId), null)
                .map(this::deserialize);
    }

    public Future<Connection> refresh(String userId, String connectionId) {
        /** If we don't have a userId, we disconnect **/
        if (userId != null) {
            return update(userId, connectionId);
        } else {
            delete(connectionId);
        }

        return Future.succeededFuture();
    }

    private String getNewConnectionId() {
        return UUID.randomUUID().toString();
    }

    private Connection deserialize(JsonObject doc) {
        try {
            return objectMapper.readValue(doc.encode(), Connection.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize connection", e);
        }
    }

    //@Override
    public Future<Boolean> isActive(String connectionId) {
        JsonObject query = new JsonObject()
                .put("_id", connectionId);

        return mongoClient.findOne(COLLECTION_NAME, query, null)
                .map(Objects::nonNull);
    }
}