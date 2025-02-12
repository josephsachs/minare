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
public class MongoContextStore implements ContextStore {
    private final MongoClient mongoClient;
    private static final String COLLECTION_NAME = "contexts";

    @Inject
    public MongoContextStore(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    @Override
    public Future<Void> addEntityToChannel(String entityId, String channelId) {
        JsonObject context = new JsonObject()
                .put("entityId", entityId)
                .put("channelId", channelId);

        return mongoClient.insert(COLLECTION_NAME, context)
                .mapEmpty();
    }

    @Override
    public Future<Void> removeEntityFromChannel(String entityId, String channelId) {
        JsonObject filter = new JsonObject()
                .put("entityId", entityId)
                .put("channelId", channelId);

        return mongoClient.removeDocument(COLLECTION_NAME, filter)
                .mapEmpty();
    }

    @Override
    public Future<Stream<JsonObject>> getChannelsForEntity(String entityId) {
        JsonObject query = new JsonObject()
                .put("entityId", entityId);

        return mongoClient.find(COLLECTION_NAME, query)
                .map(list -> list.stream());
    }

    @Override
    public Future<Stream<JsonObject>> getEntitiesInChannel(String channelId) {
        JsonObject query = new JsonObject()
                .put("channelId", channelId);

        return mongoClient.find(COLLECTION_NAME, query)
                .map(list -> list.stream());
    }
}