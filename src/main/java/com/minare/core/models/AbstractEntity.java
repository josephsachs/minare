package com.minare.core.models;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.minare.persistence.MongoUserStore;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public abstract class AbstractEntity {
    private final MongoUserStore userStore;
    protected final MongoClient mongoClient;
    protected int version;
    protected String id;
    protected Class<? extends AbstractEntity> type;
    protected Set<String> ownerIds;
    public JsonObject state;
    private static final String COLLECTION_NAME = "entities";

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    public AbstractEntity(MongoClient mongoClient, MongoUserStore userStore) {
        this.mongoClient = mongoClient;
        this.userStore = userStore;
    }

    protected boolean validate() {
        return true;
    }

    public Future<AbstractEntity> create(Class<? extends AbstractEntity> type, IEntityOwner creator, IEntityStateBuilder builder) {
        JsonObject document = new JsonObject()
                .put("type", type.getName())
                .put("version", 0)
                .put("ownerIds", creator.getId())
                .put("state", builder == null ? new JsonObject() : builder.build());

        return mongoClient.insert(COLLECTION_NAME, document)
                .compose(id -> mongoClient.findOne(COLLECTION_NAME, new JsonObject().put("_id", id), null))
                .map(doc -> {
                    try {
                        // Convert JsonObject (BSON) to a Jackson-compatible Map
                        String jsonString = doc.encode();
                        return objectMapper.readValue(jsonString, type);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to deserialize entity", e);
                    }
                });
    }

    public Future<Void> update(JsonObject updates, int expectedVersion) {
        if (expectedVersion != this.version) {
            return Future.failedFuture("Version mismatch");
        }
        if (!validate()) {
            return Future.failedFuture("Validation failed");
        }

        JsonObject query = new JsonObject()
                .put("_id", this.id)
                .put("version", expectedVersion);

        JsonObject update = new JsonObject()
                .put("$set", new JsonObject()
                        .put("state", updates)
                        .put("version", expectedVersion + 1));

        return mongoClient.updateCollection(COLLECTION_NAME, query, update)
                .map(result -> {
                    if (result.getDocMatched() == 0) {
                        throw new IllegalStateException("Entity was modified by another request");
                    }
                    this.version++;
                    return null;
                });
    }

    public Future<Void> delete(int expectedVersion) {
        if (expectedVersion != this.version) {
            return Future.failedFuture("Version mismatch");
        }

        JsonObject query = new JsonObject()
                .put("_id", this.id)
                .put("version", expectedVersion);

        return mongoClient.removeDocument(COLLECTION_NAME, query)
                .map(result -> {
                    if (result.getRemovedCount() == 0) {
                        throw new IllegalStateException("Entity was modified by another request");
                    }
                    return null;
                });
    }

    public Future<List<Channel>> getChannels() {
        JsonObject query = new JsonObject()
                .put("_id", this.id);

        return mongoClient.find("contexts", query)
                .map(docs -> docs.stream()
                        .map(doc -> new Channel(doc.getString("channelId")))
                        .collect(Collectors.toList()));
    }

    public Future<Set<IEntityOwner>> getOwners() {
        return userStore.findAll(ownerIds)
                .map(users -> users.stream()
                        .map(IEntityOwner.class::cast)
                        .collect(Collectors.toSet()));
    }
}