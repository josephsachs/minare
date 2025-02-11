package com.asyncloadtest.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.result.UpdateResult;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

@Slf4j
@Singleton
public class MongoEntityStore implements EntityStore {
    private final MongoCollection<Document> entities;

    @Inject
    public MongoEntityStore(MongoClient mongoClient) {
        this.entities = mongoClient
                .getDatabase("asyncloadtest")
                .getCollection("entities");
    }

    @Override
    public void initialize() {
        entities.createIndex(Indexes.ascending("type"));
    }

    @Override
    public void reset() {
        entities.drop();
        initialize();
    }

    @Override
    public void createEntity(String entityId, String type, JsonObject state) {
        Document entity = new Document()
                .append("_id", entityId)
                .append("type", type)
                .append("version", 0L)
                .append("state", Document.parse(state.encode()));

        entities.insertOne(entity);
    }

    @Override
    public void updateEntity(String entityId, long version, JsonObject state) {
        Document update = new Document("$set", new Document()
                .append("state", Document.parse(state.encode()))
                .append("version", version + 1));

        Document filter = new Document()
                .append("_id", entityId)
                .append("version", version);

        UpdateResult result = entities.updateOne(filter, update);

        if (result.getModifiedCount() == 0) {
            throw new IllegalStateException("Entity was modified by another request");
        }
    }

    @Override
    public JsonObject getEntity(String entityId) {
        Document doc = entities.find(new Document("_id", entityId)).first();
        if (doc == null) {
            return null;
        }
        return new JsonObject(doc.toJson());
    }

    @Override
    public Stream<JsonObject> getEntitiesByType(String type) {
        return StreamSupport.stream(
                entities.find(new Document("type", type)).spliterator(),
                false
        ).map(doc -> new JsonObject(doc.toJson()));
    }
}