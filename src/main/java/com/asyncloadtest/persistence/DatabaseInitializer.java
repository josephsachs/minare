package com.asyncloadtest.persistence;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.Indexes;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.concurrent.TimeUnit;

@Slf4j
@Singleton
public class DatabaseInitializer {
    private final MongoClient mongoClient;
    private final MongoDatabase database;

    @Inject
    public DatabaseInitializer(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
        this.database = mongoClient.getDatabase("asyncloadtest");
    }

    public void initialize() {
        if ("true".equalsIgnoreCase(System.getenv("RESET_DB"))) {
            log.info("RESET_DB=true, dropping database");
            database.drop();
        }

        // Initialize collections and indexes
        initializeConnections();
        initializeEntities();
        initializeContexts();
    }

    private void initializeConnections() {
        database.getCollection("connections")
                .createIndex(
                        Indexes.ascending("timestamp"),
                        new IndexOptions().expireAfter(60L, TimeUnit.SECONDS)
                );
        log.info("Initialized connections collection with TTL index");
    }

    private void initializeEntities() {
        database.getCollection("entities")
                .createIndex(Indexes.ascending("type"));
        log.info("Initialized entities collection with type index");
    }

    private void initializeContexts() {
        database.getCollection("contexts")
                .createIndex(Indexes.ascending("channelId"));
        log.info("Initialized contexts collection with channel index");
    }
}