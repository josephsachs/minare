package com.minare.core.state

import com.mongodb.client.model.changestream.ChangeStreamDocument
import com.mongodb.client.model.changestream.OperationType
import io.vertx.core.Vertx
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import com.minare.core.websocket.UpdateSocketManager
import com.minare.core.websocket.ConnectionManager
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class MongoChangeStreamConsumer @Inject constructor(
    private val mongoClient: MongoClient,
    private val updateSocketManager: UpdateSocketManager,
    private val connectionManager: ConnectionManager,
    private val vertx: Vertx
) {
    private val log = LoggerFactory.getLogger(MongoChangeStreamConsumer::class.java)
    private val collectionName = "entities"

    fun startConsuming() {
        val pipeline = JsonArray()
            .add(JsonObject()
                .put("\$match", JsonObject()
                    .put("operationType", JsonObject()
                        .put("\$in", JsonArray()
                            .add("update")
                            .add("insert")
                            .add("delete")
                        )
                    )
                )
            )

        mongoClient.createCollection(collectionName)
            .map {
                mongoClient.watch(
                    collectionName,
                    pipeline,
                    true,
                    1000
                )
            }
            .onSuccess { stream ->
                stream.handler(this::processChange)
                stream.exceptionHandler { error ->
                    log.error("Error in change stream", error)
                    // Attempt to restart the stream after a delay
                    vertx.setTimer(5000) { this.startConsuming() }
                }
                log.info("Started MongoDB change stream for '$collectionName' collection")
            }
            .onFailure { error ->
                log.error("Failed to start change stream", error)
                // Attempt to restart after a delay
                vertx.setTimer(5000) { this.startConsuming() }
            }
    }

    private fun processChange(changeEvent: ChangeStreamDocument<JsonObject>) {
        val operationType = changeEvent.operationType

        val updateMessage = when (operationType) {
            OperationType.UPDATE, OperationType.INSERT -> {
                val fullDocument = changeEvent.fullDocument
                if (fullDocument == null) {
                    log.warn("No full document in change event")
                    return
                }

                val entityId = fullDocument.getString("_id")
                JsonObject()
                    .put("type", "entity_update")
                    .put("entityId", entityId)
                    .put("version", fullDocument.getLong("version"))
                    .put("state", fullDocument.getJsonObject("state"))
            }
            OperationType.DELETE -> {
                val documentKey = changeEvent.documentKey
                if (documentKey == null) {
                    log.warn("No document key in delete event")
                    return
                }

                val entityId = documentKey.getString("_id")
                JsonObject()
                    .put("type", "entity_delete")
                    .put("entityId", entityId)
            }
            else -> {
                log.debug("Ignoring change event of type: {}", operationType)
                return
            }
        }

        // Broadcast to all connections
        broadcastToAllConnections(updateMessage)
    }

    private fun broadcastToAllConnections(message: JsonObject) {
        connectionManager.getAllConnectedIds()
            .onSuccess { connectionIds ->
                log.debug("Broadcasting update to {} connections", connectionIds.size)
                connectionIds.forEach { connectionId ->
                    updateSocketManager.sendUpdate(connectionId, message)
                        .onFailure { err ->
                            log.error("Failed to send update to connection {}", connectionId, err)
                        }
                }
            }
            .onFailure { err ->
                log.error("Failed to get connected clients", err)
            }
    }
}