package com.minare.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.minare.core.models.User;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class MongoUserStore implements UserStore {
    private final MongoClient mongoClient;
    private static final String COLLECTION_NAME = "users";

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Inject
    public MongoUserStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public Future<User> update(String userId, String connectionId) {
        var timestamp = System.currentTimeMillis();

        JsonObject query = new JsonObject()
                .put("_id", userId);

        JsonObject update = new JsonObject()
                .put("$set", new JsonObject()
                        .put("connectionId", connectionId)
                        .put("timestamp", timestamp));

        return mongoClient.findOneAndUpdate(COLLECTION_NAME, query, update)
                .map(updated -> {
                    if (updated == null) {
                        log.warn("No user found to update for id {}", connectionId);
                        return null;
                    }
                    log.info("Updated user {}", connectionId);
                    return hydrateUser(updated);
                });
    }

    @Override
    public Future<User> find(String userId) {
        JsonObject query = new JsonObject()
                .put("_id", userId);

        return mongoClient.findOne(COLLECTION_NAME, query, null)
                .map(this::hydrateUser);
    }

    @Override
    public Future<Set<User>> findAll(Set<String> userIds) {
        JsonObject query = new JsonObject()
                .put("_id", new JsonObject()
                        .put("$in", new JsonArray(new ArrayList<>(userIds))));

        return mongoClient.find(COLLECTION_NAME, query)
                .map(docs -> docs.stream()
                        .map(this::hydrateUser)
                        .collect(Collectors.toSet()));
    }

    private User hydrateUser(JsonObject doc) {
        try {
            return objectMapper.readValue(doc.encode(), User.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize User", e);
        }
    }
}