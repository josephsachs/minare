package com.asyncloadtest.persistence;

import io.vertx.ext.mongo.MongoClient;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.stream.Stream;

@Slf4j
@Singleton
public class MongoEntityStore implements EntityStore {
    private final MongoClient mongoClient;
    private static final String COLLECTION_NAME = "entities";

    @Inject
    public MongoEntityStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public Future<Void> createEntity(String entityId, String type, JsonObject state) {
        JsonObject entity = new JsonObject()
                .put("_id", entityId)
                .put("type", type)
                .put("version", 0L)
                .put("state", state);

        return mongoClient.insert(COLLECTION_NAME, entity)
                .mapEmpty();
    }

    @Override
    public Future<Long> updateEntity(String entityId, long version, JsonObject state) {
        JsonObject query = new JsonObject()
                .put("_id", entityId)
                .put("version", version);

        JsonObject update = new JsonObject()
                .put("$set", new JsonObject()
                        .put("state", state)
                        .put("version", version + 1));

        return mongoClient.updateCollection(COLLECTION_NAME, query, update)
                .map(result -> {
                    if (result.getDocMatched() == 0) {
                        throw new IllegalStateException("Entity was modified by another request");
                    }
                    return version + 1;
                });
    }

    @Override
    public Future<JsonObject> getEntity(String entityId) {
        JsonObject query = new JsonObject().put("_id", entityId);

        return mongoClient.findOne(COLLECTION_NAME, query, null);
    }

    @Override
    public Future<Stream<JsonObject>> getEntitiesByType(String type) {
        JsonObject query = new JsonObject().put("type", type);

        return mongoClient.find(COLLECTION_NAME, query)
                .map(list -> list.stream());
    }
}