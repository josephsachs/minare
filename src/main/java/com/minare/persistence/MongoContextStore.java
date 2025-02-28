package com.minare.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.minare.core.models.Entity;
import com.minare.core.models.Channel;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Singleton
public class MongoContextStore implements ContextStore {
    private final MongoClient mongoClient;
    private final EntityStore entityStore;
    private static final String COLLECTION_NAME = "contexts";

    private static final ObjectMapper objectMapper = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    @Inject
    public MongoContextStore(MongoClient mongoClient, EntityStore entityStore) {
        this.entityStore = entityStore;
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
    public Future<List<Channel>> getChannelsForEntity(String entityId) {
        JsonObject query = new JsonObject()
                .put("entityId", entityId);

        return mongoClient.find(COLLECTION_NAME, query)
                .map(results -> results.stream()
                        .map(this::hydrateChannel)
                        .collect(Collectors.toList()));
    }

    @Override
    public Future<List<Entity>> getEntitiesInChannel(String channelId) {
        JsonObject query = new JsonObject()
                .put("channelId", channelId);

        //return mongoClient.find("entities", query)
        //        .map(results -> results.stream()
        //                .map(entityStore::hydrateEntity)
        //                .collect(Collectors.toList()));

        throw new RuntimeException("Nope");
    }

    private Channel hydrateChannel(JsonObject doc) {
        try {
            return objectMapper.readValue(doc.encode(), Channel.class);
        } catch (Exception e) {
            throw new RuntimeException("Failed to deserialize User", e);
        }
    }
}