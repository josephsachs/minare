package com.minare.core.storage.services

import com.minare.application.config.FrameworkConfig
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import io.vertx.core.json.JsonObject

// TODO: Figure out if we want the Mongo transport adapters to remain as config options or just eliminate them

class DatabaseInitializer @Inject constructor(
    private val frameworkConfig: FrameworkConfig,
    private val mongoClient: MongoClient,
) {
    private val log = LoggerFactory.getLogger(DatabaseInitializer::class.java)

    // Collections that we manage
    private val managedCollections = listOf(
        "entity_graph"
    )

    // TODO: Handle snapshot collections here too, if mongo is selected for snapshots

    /**
     * Initialize database collections and indexes
     */
    suspend fun initialize() {
        try {
            log.info("Initializing database: ${frameworkConfig.mongo.database}")

            // Check if we should reset the database
            val shouldResetDb = checkResetDbFlag()

            if (shouldResetDb) {
                log.warn("development.reset_data flag is set to true - dropping existing collections!")
                dropExistingCollections()
            }

            // Use coroutineScope to launch concurrent initialization tasks
            coroutineScope {
                // Launch all initialization tasks concurrently
                val entitiesTask = async { initializeEntities() }
                //val connectionsTask = async { initializeConnections() }
                //val channelsTask = async { initializeChannels() }
                //val contextsTask = async { initializeContexts() }

                // Wait for all tasks to complete
                entitiesTask.await()
                //connectionsTask.await()
                //channelsTask.await()
                //contextsTask.await()
            }

            log.info("Database initialization completed successfully")
        } catch (e: Exception) {
            log.error("Database initialization failed", e)
            throw e
        }
    }

    /**
     * Check if the RESET_STATE environment variable is set to true
     */
    private fun checkResetDbFlag(): Boolean {
        return frameworkConfig.development.resetData
    }

    /**
     * Drop all existing collections that we manage
     */
    private suspend fun dropExistingCollections() {
        try {
            val existingCollections = mongoClient.getCollections().await()

            for (collection in managedCollections) {
                if (existingCollections.contains(collection)) {
                    log.warn("Dropping collection: $collection")
                    mongoClient.dropCollection(collection).await()
                    log.info("Dropped collection: $collection")
                }
            }

            log.info("Finished dropping existing collections")
        } catch (e: Exception) {
            log.error("Failed to drop existing collections", e)
            throw e
        }
    }

    private suspend fun initializeEntities() {
        try {
            val collections = mongoClient.getCollections().await()

            if (!collections.contains("entity_graph")) {
                log.info("Creating entities collection")
                mongoClient.createCollection("entity_graph").await()
            } else {
                log.debug("Entities collection already exists")
            }

            // Create indexes (idempotent operation)
            val typeIndex = JsonObject().put("type", 1)
            val indexResult = mongoClient.createIndex("entity_graph", typeIndex).await()

            log.info("Initialized entities collection with index: $indexResult")
        } catch (e: Exception) {
            log.error("Failed to initialize entities collection", e)
            throw e
        }
    }

    /**private suspend fun initializeConnections() {
        try {
            val collections = mongoClient.getCollections().await()

            if (!collections.contains("connections")) {
                log.info("Creating connections collection")
                mongoClient.createCollection("connections").await()
            } else {
                log.debug("Connections collection already exists")
            }

            // Create indexes
            val clientIdIndex = JsonObject().put("clientId", 1)
            val indexResult = mongoClient.createIndex("connections", clientIdIndex).await()

            log.info("Initialized connections collection with index: $indexResult")
        } catch (e: Exception) {
            log.error("Failed to initialize connections collection", e)
            throw e
        }
    }

    private suspend fun initializeChannels() {
        try {
            val collections = mongoClient.getCollections().await()

            if (!collections.contains("channels")) {
                log.info("Creating channels collection")
                mongoClient.createCollection("channels").await()
            } else {
                log.debug("Channels collection already exists")
            }

            log.info("Initialized channels collection")
        } catch (e: Exception) {
            log.error("Failed to initialize channels collection", e)
            throw e
        }
    }

    private suspend fun initializeContexts() {
        try {
            val collections = mongoClient.getCollections().await()

            if (!collections.contains("contexts")) {
                log.info("Creating contexts collection")
                mongoClient.createCollection("contexts").await()
            } else {
                log.debug("Contexts collection already exists")
            }

            // Create entity index
            val entityIndex = JsonObject().put("entity", 1)
            val entityIndexResult = mongoClient.createIndex("contexts", entityIndex).await()
            log.debug("Created entity index: $entityIndexResult")

            // Create channel index
            val channelIndex = JsonObject().put("channel", 1)
            val channelIndexResult = mongoClient.createIndex("contexts", channelIndex).await()
            log.debug("Created channel index: $channelIndexResult")

            log.info("Initialized contexts collection and indexes")
        } catch (e: Exception) {
            log.error("Failed to initialize contexts collection", e)
            throw e
        }
    }**/
}