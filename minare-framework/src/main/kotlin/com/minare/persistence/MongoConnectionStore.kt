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
     * This method is more resilient to race conditions.
     */
    override suspend fun delete(connectionId: String) {
        val query = JsonObject().put("_id", connectionId)

        try {
            val result = mongoClient.removeDocument(collection, query).await()
            if (result.removedCount > 0) {
                log.info("Connection deleted: {}", connectionId)
            } else {
                log.debug("Connection not found for deletion: {}", connectionId)
            }
        } catch (e: Exception) {
            log.warn("Error deleting connection: {}", connectionId, e)
            // We don't rethrow the exception to avoid cascading failures in cleanup
        }
    }

    /**
     * Check if a connection exists in the database
     * This is useful for avoiding exceptions when the connection may have been deleted
     */
    suspend fun exists(connectionId: String): Boolean {
        val query = JsonObject().put("_id", connectionId)

        try {
            val count = mongoClient.count(collection, query).await()
            return count > 0
        } catch (e: Exception) {
            log.error("Error checking if connection exists: {}", connectionId, e)
            return false
        }
    }

    /**
     * Find a connection by ID
     * Added defensive coding to handle potential race conditions
     */
    override suspend fun find(connectionId: String): Connection {
        val query = JsonObject().put("_id", connectionId)

        try {
            val result = mongoClient.findOne(collection, query, null).await()

            if (result == null) {
                log.debug("Connection not found: {}", connectionId)
                throw IllegalArgumentException("Connection not found: $connectionId")
            }

            return Connection(
                _id = result.getString("_id"),
                createdAt = result.getLong("createdAt"),
                lastUpdated = result.getLong("lastUpdated"),
                commandSocketId = result.getString("commandSocketId"),
                updateSocketId = result.getString("updateSocketId")
            )
        } catch (e: Exception) {
            if (e is IllegalArgumentException) {
                // Just rethrow our own exception
                throw e
            }
            log.error("Error finding connection: {}", connectionId, e)
            throw IllegalArgumentException("Connection not found: $connectionId")
        }
    }

    /**
     * Find a connection by ID with fallback to empty connection
     * This helps avoid exceptions in cleanup processes
     */
    suspend fun findWithFallback(connectionId: String): Connection? {
        try {
            return find(connectionId)
        } catch (e: Exception) {
            log.debug("Connection not found with fallback: {}", connectionId)
            return null
        }
    }

    /**
     * Update the lastUpdated timestamp
     */
    suspend fun updateLastUpdated(connectionId: String): Connection? {
        if (!exists(connectionId)) {
            log.debug("Connection doesn't exist for updateLastUpdated: {}", connectionId)
            return null
        }

        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject().put("lastUpdated", now))

        try {
            val result = mongoClient.updateCollection(collection, query, update).await()
            if (result.docModified == 0L) {
                log.debug("Connection not found for updateLastUpdated: {}", connectionId)
                return null
            }

            return findWithFallback(connectionId)
        } catch (e: Exception) {
            log.error("Error updating timestamp: {}", connectionId, e)
            return null
        }
    }

    /**
     * Update the update socket ID
     * More resilient to connection not being found
     */
    override suspend fun updateUpdateSocketId(connectionId: String, updateSocketId: String?): Connection {
        if (!exists(connectionId)) {
            log.debug("Connection doesn't exist for updateUpdateSocketId: {}", connectionId)
            throw IllegalArgumentException("Connection not found: $connectionId")
        }

        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject()
                .put("lastUpdated", now)
                .put("updateSocketId", updateSocketId)
            )

        try {
            val result = mongoClient.updateCollection(collection, query, update).await()
            if (result.docModified == 0L) {
                log.debug("Connection not found for updateUpdateSocketId: {}", connectionId)
                throw IllegalArgumentException("Connection not found: $connectionId")
            }

            val connection = find(connectionId)

            if (updateSocketId != null) {
                log.info("Update socket ID set for connection {}: {}", connectionId, updateSocketId)
            } else {
                log.info("Update socket ID cleared for connection {}", connectionId)
            }

            return connection
        } catch (e: Exception) {
            log.error("Error updating update socket ID: {}", connectionId, e)
            throw IllegalArgumentException("Connection not found: $connectionId")
        }
    }

    /**
     * Update the socket IDs
     * More resilient to connection not being found
     */
    override suspend fun updateSocketIds(connectionId: String, commandSocketId: String?, updateSocketId: String?): Connection {
        // Check existence first - this may fail if race condition, but better to know early
        if (!exists(connectionId)) {
            log.debug("Connection doesn't exist for updateSocketIds: {}", connectionId)
            throw IllegalArgumentException("Connection not found: $connectionId")
        }

        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject()
                .put("lastUpdated", now)
                .put("commandSocketId", commandSocketId)
                .put("updateSocketId", updateSocketId)
            )

        try {
            val result = mongoClient.updateCollection(collection, query, update).await()
            if (result.docModified == 0L) {
                log.debug("Connection not found for updateSocketIds: {}", connectionId)
                throw IllegalArgumentException("Connection not found: $connectionId")
            }

            val connection = find(connectionId)

            log.debug("Updated socket IDs for connection {}: command={}, update={}",
                connectionId, commandSocketId, updateSocketId)

            return connection
        } catch (e: Exception) {
            log.error("Error updating socket IDs: {}", connectionId, e)
            throw IllegalArgumentException("Connection not found: $connectionId")
        }
    }

    /**
     * Get all connections with update sockets
     */
    override suspend fun findAllWithUpdateSocket(): List<Connection> {
        val query = JsonObject().put("updateSocketId", JsonObject().put("\$ne", null))

        try {
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
        } catch (e: Exception) {
            log.error("Error finding connections with update socket", e)
            return emptyList()
        }
    }
}