package com.minare.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.minare.core.models.AbstractEntity;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import lombok.extern.slf4j.Slf4j;
import org.joda.time.DateTime;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class MongoEntityStore implements EntityStore {
    private final MongoClient mongoClient;
    private final ObjectMapper objectMapper;
    private static final String COLLECTION_NAME = "entities";

    @Inject
    public MongoEntityStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        this.objectMapper = new ObjectMapper()
                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    @Override
    public Future<AbstractEntity> find(String entityId) {
        JsonObject query = new JsonObject().put("_id", entityId);

        return mongoClient.findOne(COLLECTION_NAME, query, null)
                .map(this::hydrateEntity);
    }

    @Override
    public Future<Set<AbstractEntity>> findByType(String type) {
        JsonObject query = new JsonObject().put("type", type);

        return mongoClient.find(COLLECTION_NAME, query)
                .map(docs -> docs.stream()
                        .map(this::hydrateEntity)
                        .collect(Collectors.toSet()));
    }

    @Override
    public Future<Set<AbstractEntity>> findAll(Set<String> entityIds) {
        JsonObject query = new JsonObject()
                .put("_id", new JsonObject().put("$in", new JsonArray(new ArrayList<>(entityIds))));

        return mongoClient.find(COLLECTION_NAME, query)
                .map(docs -> docs.stream()
                        .map(this::hydrateEntity)
                        .collect(Collectors.toSet()));
    }

    public Future<AbstractEntity> update(String entityId, JsonObject state) {
        var timestamp = System.currentTimeMillis();

        JsonObject query = new JsonObject()
                .put("_id", entityId);

        JsonObject update = new JsonObject()
                .put("$set", new JsonObject()
                        .put("state", state)
                        .put("timestamp", timestamp))
                .put("$inc", new JsonObject()
                        .put("version", 1));

        return mongoClient.findOneAndUpdate(COLLECTION_NAME, query, update)
                .map(updated -> {
                    if (updated == null) {
                        log.warn("No entity found to update for id {}", entityId);
                        return null;
                    }
                    log.info("Updated entity {}", entityId);
                    return hydrateEntity(updated);
                });
    }

    public AbstractEntity hydrateEntity(JsonObject doc) {
        if (doc == null) {
            return null;
        }

        try {
            String type = doc.getString("type");
            Class<? extends AbstractEntity> entityClass = resolveEntityType(type);

            AbstractEntity entity = objectMapper.readValue(doc.encode(), entityClass);
            entity.state = doc.getJsonObject("state"); // Preserve raw state
            return entity;
        } catch (Exception e) {
            log.error("Failed to deserialize entity", e);
            throw new RuntimeException("Entity deserialization failed", e);
        }
    }

    private Class<? extends AbstractEntity> resolveEntityType(String type) {
        // Implement this to return the correct class dynamically
        throw new UnsupportedOperationException("resolveEntityType not implemented");
    }
}