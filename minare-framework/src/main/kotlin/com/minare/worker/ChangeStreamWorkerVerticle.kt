package com.minare.worker

import com.minare.persistence.ChannelStore
import com.minare.persistence.ContextStore
import com.minare.utils.VerticleLogger
import com.mongodb.ConnectionString
import com.mongodb.client.MongoClients
import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.FullDocument
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import org.bson.Document
import org.bson.BsonDocument
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Named

/**
 * Worker verticle that monitors MongoDB change streams and publishes normalized entity updates
 * to the event bus.
 */
class ChangeStreamWorkerVerticle @Inject constructor(
    @Named("databaseName") private val mongoDatabase: String,
    @Named("mongoConnectionString") private val mongoConnectionString: String,
    private val vlog: VerticleLogger
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(ChangeStreamWorkerVerticle::class.java)
    private val collection = "entities"
    private var running = false
    private var resumeToken: BsonDocument? = null

    companion object {

        const val ADDRESS_STREAM_STARTED = "minare.change.stream.started"
        const val ADDRESS_STREAM_STOPPED = "minare.change.stream.stopped"
        const val ADDRESS_ENTITY_UPDATED = "minare.entity.update"


        const val CHANGE_BATCH_SIZE = 100
        const val MAX_WAIT_MS = 200L
    }

    override suspend fun start() {
        try {
            running = true


            launch {
                processChangeStream()
            }

            log.info("ChangeStreamWorkerVerticle started successfully")
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
     * Main method to process the MongoDB change stream.
     * Uses the MongoDB driver to watch for changes and publishes normalized events
     * to the event bus.
     */
    private suspend fun processChangeStream() {

        val mongoJob = SupervisorJob()
        val mongoContext = Dispatchers.IO + mongoJob

        try {
            // Connect to MongoDB with the native driver
            val (nativeMongoClient, entitiesCollection) = withContext(mongoContext) {
                val mongoClientSettings = com.mongodb.MongoClientSettings.builder()
                    .applyConnectionString(ConnectionString(mongoConnectionString))
                    .build()

                val client = MongoClients.create(mongoClientSettings)
                val database = client.getDatabase(mongoDatabase)
                val collection = database.getCollection(collection)

                Pair(client, collection)
            }

            // Use pipeline to filter only relevant changes
            val pipeline = listOf(
                Document("\$match",
                    Document("operationType",
                        Document("\$in", listOf("insert", "update", "replace"))
                    )
                )
            )

            vertx.eventBus().publish(ADDRESS_STREAM_STARTED, true)
            log.info("Change stream listener started for collection: $collection")

            try {
                // The change stream watching happens in the IO context
                withContext(mongoContext) {
                    // Configure the change stream
                    var changeStreamIterable = entitiesCollection.watch(pipeline)
                        .fullDocument(FullDocument.UPDATE_LOOKUP)

                    // Use resume token if available
                    if (resumeToken != null) {
                        changeStreamIterable = changeStreamIterable.resumeAfter(resumeToken)
                    }

                    val changeStream = changeStreamIterable.iterator()

                    // For batch processing
                    val batch = mutableListOf<ChangeStreamDocument<Document>>()
                    var lastProcessTime = System.currentTimeMillis()

                    while (isActive && running) {
                        try {
                            // Try to get the next change or null if none is immediately available
                            val change = changeStream.tryNext()

                            if (change != null) {
                                batch.add(change)
                                // Store resume token
                                resumeToken = change.resumeToken
                            }

                            val batchFull = batch.size >= CHANGE_BATCH_SIZE
                            val timeThresholdExceeded = batch.isNotEmpty() &&
                                    (System.currentTimeMillis() - lastProcessTime > MAX_WAIT_MS)

                            // Process batch if it's full or we've waited long enough
                            if (batchFull || timeThresholdExceeded) {
                                val changesToProcess = ArrayList(batch) // Make a copy
                                batch.clear()

                                // Switch back to Vert.x context for processing
                                withContext(vertx.dispatcher()) {
                                    processBatch(changesToProcess)
                                }

                                lastProcessTime = System.currentTimeMillis()
                            } else if (change == null) {
                                // No new changes, small delay to avoid tight loop
                                delay(10)
                            }
                        } catch (e: Exception) {
                            if (e is CancellationException) throw e
                            log.error("Error processing change event", e)
                            delay(1000) // Delay before retry
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

            // If we get here naturally, try to restart
            if (running) {
                log.warn("Change stream loop exited unexpectedly, restarting...")
                delay(1000)
                processChangeStream() // Recursive restart
            }

        } catch (e: Exception) {
            if (e is CancellationException) throw e
            log.error("Error in change stream watcher", e)

            // Try to restart after a delay if we're still running
            if (running) {
                delay(5000)
                processChangeStream() // Recursive restart
            }
        }
    }

    /**
     * Process a batch of change stream documents
     */
    private suspend fun processBatch(batch: List<ChangeStreamDocument<Document>>) {
        val startTime = System.currentTimeMillis()
        log.debug("Processing batch of ${batch.size} changes")

        try {
            // Convert and publish each change
            for (change in batch) {
                try {
                    val entityUpdate = normalizeChangeEvent(change)
                    if (entityUpdate != null) {
                        // Publish to event bus for the UpdateVerticle to consume
                        vertx.eventBus().publish(ADDRESS_ENTITY_UPDATED, entityUpdate)
                    }
                } catch (e: Exception) {
                    log.error("Error processing change: ${e.message}", e)
                    // Continue with next change
                }
            }

            val processTime = System.currentTimeMillis() - startTime
            log.debug("Processed ${batch.size} changes in ${processTime}ms")

        } catch (e: Exception) {
            log.error("Error processing change batch", e)
        }
    }

    /**
     * Converts a MongoDB change event to a normalized entity update format
     */
    private fun normalizeChangeEvent(changeDocument: ChangeStreamDocument<Document>): JsonObject? {
        try {
            val operationType = changeDocument.operationType?.value ?: return null
            if (operationType !in listOf("insert", "update", "replace")) {
                return null
            }

            val documentKey = changeDocument.documentKey ?: return null
            val entityId = getBsonValueAsString(documentKey["_id"]) ?: return null

            val fullDocument = changeDocument.fullDocument ?: return null

            // Extract entity type
            val type = fullDocument.getString("type") ?: return null

            // Extract version - handle different potential formats
            val version = when (val versionValue = fullDocument["version"]) {
                is Int -> versionValue.toLong()
                is Long -> versionValue
                is String -> try { versionValue.toLong() } catch (e: Exception) { 1L }
                else -> 1L
            }

            // Extract entity state
            val state = fullDocument.get("state", Document::class.java)
            if (state == null) {
                log.warn("Entity $entityId has no state field")
                return null
            }

            // Create normalized entity update
            return JsonObject()
                .put("type", type)
                .put("_id", entityId)
                .put("version", version)
                .put("operation", operationType)
                .put("changedAt", System.currentTimeMillis())
                .put("state", JsonObject(state.toJson()))
        } catch (e: Exception) {
            log.error("Error normalizing change event: ${e.message}", e)
            return null
        }
    }

    /**
     * Helper method to extract string representation from BSON value
     */
    private fun getBsonValueAsString(value: Any?): String? {
        return when (value) {
            is org.bson.BsonObjectId -> value.value.toHexString()
            is org.bson.types.ObjectId -> value.toHexString()
            is String -> value
            else -> value?.toString()
        }
    }
}