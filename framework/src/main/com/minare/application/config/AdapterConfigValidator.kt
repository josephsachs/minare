package com.minare.application.config

import com.minare.exceptions.ConfigurationException
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.await

/**
 * Validate any adapters and connections configured for the framework.
 */
class AdapterConfigValidator {
    suspend fun validate(vertx: Vertx, config: FrameworkConfig) {
        if (config.mongo.configured) {
            val isValid = validateMongo(vertx, config)

            config.mongo.enabled = if (isValid) {
                true
            } else {
                throw ConfigurationException("Mongo database is configured but connection is refused, ensure URI and port are valid and that service is available")
            }
        }
    }

    /**
     * Test if a Mongo connection is possible to establish with the given configuration
     */
    private suspend fun validateMongo(vertx: Vertx, config: FrameworkConfig): Boolean {
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