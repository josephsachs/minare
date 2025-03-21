package com.minare.worker

import com.minare.core.websocket.UpdateSocketManager
import com.minare.persistence.ChannelStore
import com.minare.persistence.ContextStore
import com.mongodb.ConnectionString
import com.mongodb.client.MongoClients
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.FullDocument
import io.vertx.core.Promise
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.bson.Document
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Named

class ChangeStreamWorkerVerticle @Inject constructor(
    private val contextStore: ContextStore,
    private val channelStore: ChannelStore,
    private val updateSocketManager: UpdateSocketManager,
    @Named("databaseName") private val mongoDatabase: String,
    @Named("mongoConnectionString") private val mongoConnectionString: String
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(ChangeStreamWorkerVerticle::class.java)
    private val collection = "entities"
    private var running = false

    companion object {
        const val ADDRESS_STREAM_STARTED = "change.stream.started"
        const val ADDRESS_STREAM_STOPPED = "change.stream.stopped"
        const val ADDRESS_ENTITY_UPDATED = "entity.updated"
    }

    override suspend fun start() {
        try {
            running = true

            // Create dedicated IO Job for blocking MongoDB operations
            val mongoJob = SupervisorJob()
            val mongoContext = Dispatchers.IO + mongoJob

            // Launch the change stream watcher
            launch {
                try {
                    // Connect to MongoDB (this is IO-bound work)
                    val (nativeMongoClient, entitiesCollection) = withContext(mongoContext) {
                        // Use the native MongoDB Java driver
                        val mongoClientSettings = com.mongodb.MongoClientSettings.builder()
                            .applyConnectionString(ConnectionString(mongoConnectionString))
                            .build()

                        val client = MongoClients.create(mongoClientSettings)
                        val database = client.getDatabase(mongoDatabase)
                        val collection = database.getCollection(collection)

                        Pair(client, collection)
                    }

                    // Signal that we're started
                    vertx.eventBus().publish(ADDRESS_STREAM_STARTED, true)
                    log.info("Change stream listener started for collection: $collection")

                    try {
                        // The change stream watching happens in the IO context
                        withContext(mongoContext) {
                            val changeStream = entitiesCollection.watch()
                                .fullDocument(FullDocument.UPDATE_LOOKUP)

                            val iterator = changeStream.iterator()

                            while (isActive && running && iterator.hasNext()) {
                                try {
                                    // This is a blocking call, but we're in the IO context
                                    val changeDocument = iterator.next()

                                    // Convert to JsonObject
                                    val changeEvent = convertChangeDocumentToJsonObject(changeDocument)

                                    // Switch back to Vert.x context for processing
                                    withContext(vertx.dispatcher()) {
                                        // Process the event
                                        processChangeEvent(changeEvent)

                                        // Also publish to event bus
                                        vertx.eventBus().publish(ADDRESS_ENTITY_UPDATED, changeEvent)
                                    }
                                } catch (e: Exception) {
                                    log.error("Error processing change event", e)
                                }
                            }
                        }
                    } finally {
                        // Clean up MongoDB client
                        withContext(mongoContext) {
                            try {
                                nativeMongoClient.close()
                            } catch (e: Exception) {
                                log.error("Error closing MongoDB client", e)
                            }
                        }
                    }

                    // If we get here, the change stream has stopped
                    if (running) {
                        log.warn("Change stream loop exited unexpectedly, restarting...")
                        // Restart after a delay
                        vertx.setTimer(1000) {
                            launch { start() }
                        }
                    }
                } catch (e: Exception) {
                    log.error("Error in change stream watcher", e)

                    // Try to restart after a delay
                    if (running) {
                        vertx.setTimer(1000) {
                            launch { start() }
                        }
                    }
                }
            }
        } catch (e: Exception) {
            log.error("Failed to start change stream listener", e)
            throw e
        }
    }

    override suspend fun stop() {
        running = false
        vertx.eventBus().publish(ADDRESS_STREAM_STOPPED, true)
        log.info("Change stream listener stopped")
    }

    /**
     * Converts a MongoDB ChangeStreamDocument to a Vert.x JsonObject
     */
    /**
     * Converts a MongoDB ChangeStreamDocument to a Vert.x JsonObject
     */
    /**
     * Converts a MongoDB ChangeStreamDocument to a Vert.x JsonObject
     */
    private fun convertChangeDocumentToJsonObject(changeDocument: ChangeStreamDocument<Document>): JsonObject {
        val result = JsonObject()

        // Add operation type
        result.put("operationType", changeDocument.operationType?.value)

        // Add document key
        val documentKey = JsonObject()
        changeDocument.documentKey?.forEach { key, value ->
            // Handle BSON ObjectId values specially
            if (value is org.bson.BsonObjectId) {
                documentKey.put(key, value.getValue().toHexString())
            } else {
                documentKey.put(key, value.toString())
            }
        }
        result.put("documentKey", documentKey)

        // Add full document if available
        changeDocument.fullDocument?.let { document ->
            val fullDoc = JsonObject()
            document.forEach { key, value ->
                when (value) {
                    is Document -> fullDoc.put(key, JsonObject(value.toJson()))
                    is Number -> fullDoc.put(key, value)  // Preserve numeric types
                    is org.bson.types.ObjectId -> fullDoc.put(key, value.toHexString())
                    else -> fullDoc.put(key, value.toString())
                }
            }
            result.put("fullDocument", fullDoc)
        }

        return result
    }

    /**
     * Processes a single change event from MongoDB
     * @param event The change event from MongoDB
     */
    private suspend fun processChangeEvent(event: JsonObject) {
        // Extract entity information
        val operationType = event.getString("operationType")
        if (operationType != "insert" && operationType != "update" && operationType != "replace") {
            return // Skip other operations like delete
        }

        val documentKey = event.getJsonObject("documentKey")
        val entityId = documentKey?.getString("_id") ?: return

        val fullDocument = event.getJsonObject("fullDocument") ?: return

        // Handle version value that might be a string or a number
        val version = when (val versionValue = fullDocument.getValue("version")) {
            is Number -> versionValue.toLong()
            is String -> try { versionValue.toLong() } catch (e: Exception) { 0L }
            else -> 0L
        }

        val state = fullDocument.getJsonObject("state") ?: JsonObject()

        try {
            // These are suspend calls but we're in the Vert.x coroutine context
            val channelIds = contextStore.getChannelsByEntityId(entityId)

            if (channelIds.isEmpty()) {
                return // No channels subscribed to this entity
            }

            val updateMessage = createUpdateMessage(entityId, version, state)

            // Get clients for all channels and broadcast the update
            val clientIds = channelIds.flatMap { channelId ->
                channelStore.getClientIds(channelId)
            }

            if (clientIds.isNotEmpty()) {
                updateSocketManager.broadcastUpdate(clientIds, updateMessage)
            }
        } catch (e: Exception) {
            log.error("Error broadcasting update for entity $entityId", e)
        }
    }

    /**
     * Creates a properly formatted update message
     */
    private fun createUpdateMessage(entityId: String, version: Long, state: JsonObject): JsonObject {
        val entityUpdate = JsonObject()
            .put("id", entityId)
            .put("version", version)
            .put("state", state)

        val entitiesArray = JsonArray().add(entityUpdate)

        return JsonObject()
            .put("update", JsonObject()
                .put("entities", entitiesArray)
            )
    }
}