package com.asyncloadtest.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Indexes;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j

@Singleton
public class MongoContextStore implements ContextStore {
    private final MongoCollection<Document> collection;

    @Inject
    public MongoContextStore(MongoClient mongoClient) {
        this.collection = mongoClient.getDatabase("asyncloadtest")
                .getCollection("contexts");
    }

    @Override
    public void addEntityToChannel(String entityId, String channelId) {
        Document context = new Document()
                .append("entityId", entityId)
                .append("channelId", channelId);

        collection.insertOne(context);
    }

    @Override
    public void removeEntityFromChannel(String entityId, String channelId) {
        Document filter = new Document()
                .append("entityId", entityId)
                .append("channelId", channelId);

        collection.deleteOne(filter);
    }

    @Override
    public Stream<JsonObject> getChannelsForEntity(String entityId) {
        return StreamSupport.stream(
                collection.find(new Document("entityId", entityId)).spliterator(),
                false
        ).map(doc -> new JsonObject(doc.toJson()));
    }

    @Override
    public Stream<JsonObject> getEntitiesInChannel(String channelId) {
        return StreamSupport.stream(
                collection.find(new Document("channelId", channelId)).spliterator(),
                false
        ).map(doc -> new JsonObject(doc.toJson()));
    }
}