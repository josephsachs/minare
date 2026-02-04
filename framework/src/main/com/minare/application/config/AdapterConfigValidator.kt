package com.minare.application.config

import com.minare.core.frames.services.SnapshotService.Companion.SnapshotStoreOption
import com.minare.core.storage.interfaces.EntityGraphStoreOption
import com.minare.exceptions.ConfigurationException
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory

/**
 * Validate any adapters and connections configured for the framework.
 */
class AdapterConfigValidator {
    private val log = LoggerFactory.getLogger(AdapterConfigValidator::class.java)

    suspend fun validate(vertx: Vertx, config: FrameworkConfig) {
        validateMongo(vertx, config)
    }

    /**
     * If any adapters are set to use Mongo, ensure it is available
     */
    private suspend fun validateMongo(vertx: Vertx, config: FrameworkConfig) {
        val mongoWarning = "Mongo configuration is missing or incomplete. Ensure that a mongo section exists in the " +
            "configuration file and that mongo.host, mongo.port and mongo.database are specified."

        if (config.mongo.configured.not()) {
            if (config.frames.snapshot.store == SnapshotStoreOption.MONGO) {
                log.error("Snapshots are set to use Mongo. $mongoWarning")
                throw ConfigurationException("Selected adapter missing configuration")
            }

            if (config.entity.graph.store == EntityGraphStoreOption.MONGO) {
                log.error("Entity graph store is set to use Mongo. $mongoWarning")
                throw ConfigurationException("Selected adapter missing configuration")
            }
        }

        if (config.mongo.configured) {
            val isValid = testMongoConnection(vertx, config)

            config.mongo.enabled = if (isValid) {
                true
            } else {
                log.error("Mongo database is configured but connection is refused, ensure that URI and port are valid and that service is available.")
                throw ConfigurationException("Mongo refused connection")
            }
        }
    }

    /**
     * Test if a Mongo connection is possible to establish with the given configuration
     */
    private suspend fun testMongoConnection(vertx: Vertx, config: FrameworkConfig): Boolean {
        val mongoUri = "mongodb://${config.mongo.host}:${config.mongo.port}"
        val dbName = config.mongo.database

        val client = MongoClient.create(vertx, JsonObject()
            .put("connection_string", mongoUri)
            .put("db_name", dbName))

        return try {
            val result = client.runCommand("ping", JsonObject().put("ping", 1)).await()
            client.close()
            true
        } catch (e: Exception) {
            client.close()
            false
        }
    }
}