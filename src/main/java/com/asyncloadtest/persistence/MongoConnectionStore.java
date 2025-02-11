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
    private final MongoCollection<Document> connections;

    @Inject
    public MongoConnectionStore(MongoClient mongoClient) {
        this.connections = mongoClient
                .getDatabase("asyncloadtest")
                .getCollection("connections");
    }

    @Override
    public void initialize() {
        // Create TTL index
        connections.createIndex(
                Indexes.ascending("timestamp"),
                new IndexOptions()
                        .expireAfter(60L, TimeUnit.SECONDS)  // 1 minute TTL
        );
    }

    @Override
    public void reset() {
        connections.drop();
        initialize();
    }

    @Override
    public void storeConnection(String connectionId, long timestamp) {
        Document connection = new Document()
                .append("_id", connectionId)
                .append("timestamp", new Date(timestamp));

        connections.insertOne(connection);
        log.info("Stored connection {}", connectionId);
    }

    @Override
    public void removeConnection(String connectionId) {
        connections.deleteOne(new Document("_id", connectionId));
        log.info("Removed connection {}", connectionId);
    }

    @Override
    public boolean isConnectionActive(String connectionId) {
        return connections.find(new Document("_id", connectionId))
                .first() != null;
    }
}