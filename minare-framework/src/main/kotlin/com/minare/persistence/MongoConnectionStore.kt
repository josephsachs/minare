package com.minare.persistence

import com.minare.core.models.Connection
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory
import java.util.UUID
import javax.inject.Inject
import javax.inject.Singleton

@Singleton
class MongoConnectionStore @Inject constructor(
    private val mongoClient: MongoClient
) : ConnectionStore {
    private val log = LoggerFactory.getLogger(MongoConnectionStore::class.java)
    private val collection = "connections"

    /**
     * Create a new connection in the database
     */
    override suspend fun create(): Connection {
        val connectionId = UUID.randomUUID().toString()
        val now = System.currentTimeMillis()

        val connection = Connection(
            _id = connectionId,
            createdAt = now,
            lastUpdated = now,
            commandSocketId = null,
            updateSocketId = null
        )

        val document = JsonObject()
            .put("_id", connection._id)
            .put("createdAt", connection.createdAt)
            .put("lastUpdated", connection.lastUpdated)
            .put("commandSocketId", connection.commandSocketId)
            .put("updateSocketId", connection.updateSocketId)

        try {
            mongoClient.save(collection, document).await()
            log.info("Connection created: {}", connectionId)
            return connection
        } catch (e: Exception) {
            log.error("Failed to create connection", e)
            throw e
        }
    }

    /**
     * Delete a connection
     */
    override suspend fun delete(connectionId: String) {
        val query = JsonObject().put("_id", connectionId)

        try {
            mongoClient.removeDocument(collection, query).await()
            log.info("Connection deleted: {}", connectionId)
        } catch (e: Exception) {
            log.error("Failed to delete connection: {}", connectionId, e)
            throw e
        }
    }

    /**
     * Delete a connection by connection ID (not part of the interface)
     */
    suspend fun deleteByConnectionId(connectionId: String) {
        val query = JsonObject().put("_id", connectionId)

        try {
            val result = mongoClient.removeDocument(collection, query).await()
            if (result.removedCount > 0) {
                log.info("Connection deleted by ID: {}", connectionId)
            } else {
                log.warn("Connection not found for deletion by ID: {}", connectionId)
            }
        } catch (e: Exception) {
            log.error("Failed to delete connection by ID: {}", connectionId, e)
            throw e
        }
    }

    /**
     * Find a connection by ID
     */
    override suspend fun find(connectionId: String): Connection {
        val query = JsonObject().put("_id", connectionId)

        val result = mongoClient.findOne(collection, query, null).await()
            ?: throw IllegalArgumentException("Connection not found: $connectionId")

        return Connection(
            _id = result.getString("_id"),
            createdAt = result.getLong("createdAt"),
            lastUpdated = result.getLong("lastUpdated"),
            commandSocketId = result.getString("commandSocketId"),
            updateSocketId = result.getString("updateSocketId")
        )
    }

    /**
     * Update the lastUpdated timestamp (not part of the interface)
     */
    suspend fun updateLastUpdated(connectionId: String): Connection {
        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject().put("lastUpdated", now))

        val result = mongoClient.updateCollection(collection, query, update).await()
        if (result.docModified == 0L) {
            throw IllegalArgumentException("Connection not found: $connectionId")
        }

        return find(connectionId)
    }

    /**
     * Update the update socket ID (implementation of interface method)
     */
    override suspend fun updateUpdateSocketId(connectionId: String, updateSocketId: String?): Connection {
        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject()
                .put("lastUpdated", now)
                .put("updateSocketId", updateSocketId)
            )

        val result = mongoClient.updateCollection(collection, query, update).await()
        if (result.docModified == 0L) {
            throw IllegalArgumentException("Connection not found: $connectionId")
        }

        val connection = find(connectionId)

        if (updateSocketId != null) {
            log.info("Update socket ID set for connection {}: {}", connectionId, updateSocketId)
        } else {
            log.info("Update socket ID cleared for connection {}", connectionId)
        }

        return connection
    }

    /**
     * Update the socket IDs
     */
    override suspend fun updateSocketIds(connectionId: String, commandSocketId: String?, updateSocketId: String?): Connection {
        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject()
                .put("lastUpdated", now)
                .put("commandSocketId", commandSocketId)
                .put("updateSocketId", updateSocketId)
            )

        val result = mongoClient.updateCollection(collection, query, update).await()
        if (result.docModified == 0L) {
            throw IllegalArgumentException("Connection not found: $connectionId")
        }

        val connection = find(connectionId)

        if (updateSocketId != null) {
            log.info("Update socket ID set for connection {}: {}", connectionId, updateSocketId)
        } else {
            log.info("Update socket ID cleared for connection {}", connectionId)
        }

        return connection
    }

    /**
     * Get all connections with update sockets
     */
    override suspend fun findAllWithUpdateSocket(): List<Connection> {
        val query = JsonObject().put("updateSocketId", JsonObject().put("\$ne", null))

        val documents = mongoClient.find(collection, query).await()
        return documents.map { doc ->
            Connection(
                _id = doc.getString("_id"),
                createdAt = doc.getLong("createdAt"),
                lastUpdated = doc.getLong("lastUpdated"),
                commandSocketId = doc.getString("commandSocketId"),
                updateSocketId = doc.getString("updateSocketId")
            )
        }
    }
}