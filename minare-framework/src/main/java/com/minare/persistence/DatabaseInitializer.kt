package com.minare.persistence

import io.vertx.core.Future
import io.vertx.ext.mongo.MongoClient
import io.vertx.ext.mongo.IndexOptions
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class DatabaseInitializer @Inject constructor(
    private val mongoClient: MongoClient
) {
    private val log = LoggerFactory.getLogger(DatabaseInitializer::class.java)
    private val dbName = "minare"

    /**
     * Initialize the database collections and indexes.
     * If RESET_DB environment variable is set to true, all collections will be dropped first.
     */
    fun initialize(): Future<Void> {
        return if ("true".equals(System.getenv("RESET_DB"), ignoreCase = true)) {
            log.info("RESET_DB=true, dropping collections")
            mongoClient.dropCollection("connections")
                .compose { mongoClient.dropCollection("entities") }
                .compose { initializeAllCollections() }
        } else {
            initializeAllCollections()
        }
    }

    private fun initializeAllCollections(): Future<Void> {
        return initializeConnections()
            .compose { initializeEntities() }
    }

    private fun initializeConnections(): Future<Void> {
        val index = JsonObject()
            .put("lastUpdated", 1)

        val indexOptions = IndexOptions()
            .name("lastUpdated_ttl_idx")
            .expireAfter(60L, TimeUnit.MINUTES) // Connections expire after 60 minutes of inactivity

        return mongoClient.createCollection("connections")
            .compose { mongoClient.createIndexWithOptions("connections", index, indexOptions) }
            .onSuccess { log.info("Initialized connections collection with TTL index") }
            .mapEmpty()
    }

    private fun initializeEntities(): Future<Void> {
        val typeIndex = JsonObject()
            .put("type", 1)

        val versionIndex = JsonObject()
            .put("version", 1)

        return mongoClient.createCollection("entities")
            .compose { mongoClient.createIndex("entities", typeIndex) }
            .compose { mongoClient.createIndex("entities", versionIndex) }
            .onSuccess { log.info("Initialized entities collection with indexes") }
            .mapEmpty()
    }
}