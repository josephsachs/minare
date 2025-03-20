package com.minare.core.state

import com.minare.core.websocket.UpdateSocketManager
import com.minare.persistence.ChannelStore
import com.minare.persistence.ContextStore
import com.mongodb.ConnectionString
import com.mongodb.client.MongoClients
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.FullDocument
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.bson.Document
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Named
import javax.inject.Singleton

@Singleton
class MongoEntityStreamConsumer @Inject constructor(
    private val contextStore: ContextStore,
    private val channelStore: ChannelStore,
    private val updateSocketManager: UpdateSocketManager,
    private val vertx: Vertx,
    @Named("databaseName") private val mongoDatabase: String,
    @Named("mongoConnectionString") private val mongoConnectionString: String
) {
    private val collection = "entities"
    private val log = LoggerFactory.getLogger(MongoEntityStreamConsumer::class.java)

    // Create a supervised coroutine scope tied to this consumer's lifecycle
    private val consumerScope = CoroutineScope(vertx.dispatcher() + SupervisorJob())

    /**
     * Starts listening to the MongoDB change stream
     */
    suspend fun listen() {
        try {
            // Use the native MongoDB Java driver
            val mongoClientSettings = com.mongodb.MongoClientSettings.builder()
                .applyConnectionString(ConnectionString(mongoConnectionString))
                .build()

            val nativeMongoClient = MongoClients.create(mongoClientSettings)
            val database = nativeMongoClient.getDatabase(mongoDatabase)
            val entitiesCollection = database.getCollection(collection)

            consumerScope.launch {
                try {
                    val changeStream = entitiesCollection.watch()
                        .fullDocument(FullDocument.UPDATE_LOOKUP)

                    val iterator = changeStream.iterator()
                    while (isActive && iterator.hasNext()) {
                        val changeDocument = iterator.next()

                        val changeEvent = convertChangeDocumentToJsonObject(changeDocument)

                        try {
                            processChangeEvent(changeEvent)
                        } catch (e: Exception) {
                            log.error("Error processing change event", e)
                        }
                    }
                } catch (e: Exception) {
                    log.error("Error in change stream watcher", e)
                    kotlinx.coroutines.delay(1000)
                    listen()
                } finally {
                    nativeMongoClient.close()
                }
            }

            log.info("Started listening to change stream for collection: $collection")
        } catch (e: Exception) {
            log.error("Failed to start change stream listener", e)
            throw e
        }
    }

    /**
     * Stops the change stream listener
     */
    fun stopListening() {
        consumerScope.cancel("Stream consumer stopping")
    }

    /**
     * Converts a MongoDB ChangeStreamDocument to a Vert.x JsonObject
     */
    private fun convertChangeDocumentToJsonObject(changeDocument: ChangeStreamDocument<Document>): JsonObject {
        val result = JsonObject()

        // Add operation type
        result.put("operationType", changeDocument.operationType.value)

        // Add document key
        val documentKey = JsonObject()
        changeDocument.documentKey?.forEach { key, value ->
            documentKey.put(key, value.toString())
        }
        result.put("documentKey", documentKey)

        // Add full document if available
        changeDocument.fullDocument?.let { document ->
            val fullDoc = JsonObject()
            document.forEach { key, value ->
                when (value) {
                    is Document -> fullDoc.put(key, JsonObject(value.toJson()))
                    is Number -> fullDoc.put(key, value)  // Preserve numeric types
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
    suspend fun processChangeEvent(event: JsonObject) {
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