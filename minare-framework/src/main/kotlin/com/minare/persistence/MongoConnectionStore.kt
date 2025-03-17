package com.minare.persistence

import com.minare.core.models.Connection
import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import org.slf4j.LoggerFactory
import java.util.UUID
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class MongoConnectionStore @Inject constructor(
    private val mongoClient: MongoClient
) {
    private val log = LoggerFactory.getLogger(MongoConnectionStore::class.java)
    private val collection = "connections"

    /**
     * Create a new connection in the database
     */
    fun create(): Future<Connection> {
        val connectionId = UUID.randomUUID().toString()
        val now = System.currentTimeMillis()

        val connection = Connection(
            id = connectionId,
            createdAt = now,
            lastUpdated = now,
            commandSocketId = null,
            updateSocketId = null
        )

        val document = JsonObject()
            .put("_id", connection.id)
            .put("createdAt", connection.createdAt)
            .put("lastUpdated", connection.lastUpdated)
            .put("commandSocketId", connection.commandSocketId)
            .put("updateSocketId", connection.updateSocketId)

        return mongoClient.save(collection, document)
            .map { connection }
            .onSuccess { log.info("Connection created: {}", connectionId) }
            .onFailure { err -> log.error("Failed to create connection", err) }
    }

    /**
     * Delete a connection
     */
    fun delete(connectionId: String): Future<Nothing?> {
        val query = JsonObject().put("_id", connectionId)

        return mongoClient.removeDocument(collection, query)
            .map { null }
            .onSuccess { log.info("Connection deleted: {}", connectionId) }
            .onFailure { err -> log.error("Failed to delete connection: {}", connectionId, err) }
    }

    /**
     * Find a connection by ID
     */
    fun find(connectionId: String): Future<Connection> {
        val query = JsonObject().put("_id", connectionId)

        return mongoClient.findOne(collection, query, null)
            .compose { result ->
                if (result == null) {
                    Future.failedFuture("Connection not found: $connectionId")
                } else {
                    Future.succeededFuture(
                        Connection(
                            id = result.getString("_id"),
                            createdAt = result.getLong("createdAt"),
                            lastUpdated = result.getLong("lastUpdated"),
                            commandSocketId = result.getString("commandSocketId"),
                            updateSocketId = result.getString("updateSocketId")
                        )
                    )
                }
            }
    }

    /**
     * Update the lastUpdated timestamp
     */
    fun updateLastUpdated(connectionId: String): Future<Connection> {
        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject().put("lastUpdated", now))

        return mongoClient.updateCollection(collection, query, update)
            .compose { result ->
                if (result.docModified == 0L) {
                    Future.failedFuture("Connection not found: $connectionId")
                } else {
                    find(connectionId)
                }
            }
    }

    /**
     * Update the update socket ID
     */
    fun updateUpdateSocketId(connectionId: String, commandSocketId: String?, updateSocketId: String?): Future<Connection> {
        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject()
                .put("lastUpdated", now)
                .put("commandSocketId", commandSocketId)
                .put("updateSocketId", updateSocketId)
            )

        return mongoClient.updateCollection(collection, query, update)
            .compose { result ->
                if (result.docModified == 0L) {
                    Future.failedFuture("Connection not found: $connectionId")
                } else {
                    find(connectionId)
                }
            }
            .onSuccess {
                if (updateSocketId != null) {
                    log.info("Update socket ID set for connection {}: {}", connectionId, updateSocketId)
                } else {
                    log.info("Update socket ID cleared for connection {}", connectionId)
                }
            }
    }

    /**
     * Get all connections with update sockets
     */
    fun findAllWithUpdateSocket(): Future<List<Connection>> {
        val query = JsonObject().put("updateSocketId", JsonObject().put("\$ne", null))

        return mongoClient.find(collection, query)
            .map { documents ->
                documents.map { doc ->
                    Connection(
                        id = doc.getString("_id"),
                        createdAt = doc.getLong("createdAt"),
                        lastUpdated = doc.getLong("lastUpdated"),
                        commandSocketId = doc.getString("commandSocketId"),
                        updateSocketId = doc.getString("updateSocketId")
                    )
                }
            }
    }
}