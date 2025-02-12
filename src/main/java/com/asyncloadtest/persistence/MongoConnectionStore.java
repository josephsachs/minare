package com.asyncloadtest.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import org.bson.Document;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Date;
import java.util.concurrent.TimeUnit;

@Slf4j
@Singleton
public class MongoConnectionStore implements ConnectionStore {
    private final MongoCollection<Document> collection;

    @Inject
    public MongoConnectionStore(MongoClient mongoClient) {
        this.collection = mongoClient.getDatabase("asyncloadtest")
                .getCollection("connections");
    }

    @Override
    public void storeConnection(String connectionId, long timestamp) {
        Document connection = new Document()
                .append("_id", connectionId)
                .append("timestamp", new Date(timestamp));

        collection.insertOne(connection);
        log.info("Stored connection {}", connectionId);
    }

    @Override
    public void removeConnection(String connectionId) {
        collection.deleteOne(new Document("_id", connectionId));
        log.info("Removed connection {}", connectionId);
    }

    @Override
    public boolean isConnectionActive(String connectionId) {
        return collection.find(new Document("_id", connectionId))
                .first() != null;
    }
}
