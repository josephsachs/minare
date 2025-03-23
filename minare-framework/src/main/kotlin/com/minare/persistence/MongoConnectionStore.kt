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
            lastActivity = now,
            commandSocketId = null,
            updateSocketId = null,
            reconnectable = true
        )

        val document = JsonObject()
            .put("_id", connection._id)
            .put("createdAt", connection.createdAt)
            .put("lastUpdated", connection.lastUpdated)
            .put("lastActivity", connection.lastActivity)
            .put("commandSocketId", connection.commandSocketId)
            .put("updateSocketId", connection.updateSocketId)
            .put("reconnectable", connection.reconnectable)

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
                throw IllegalStateException("Connection not found: $connectionId")
            }
        } catch (e: Exception) {
            log.error("Error deleting connection: {}", connectionId, e)
            throw e
        }
    }

    /**
     * Check if a connection exists in the database
     * This is useful for avoiding exceptions when the connection may have been deleted
     */
    override suspend fun exists(connectionId: String): Boolean {
        val query = JsonObject().put("_id", connectionId)

        try {
            val count = mongoClient.count(collection, query).await()
            return count > 0
        } catch (e: Exception) {
            log.error("Error checking if connection exists: {}", connectionId, e)
            throw e
        }
    }

    /**
     * Find a connection by ID
     * Improved error handling without fallbacks
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
                lastActivity = result.getLong("lastActivity", result.getLong("lastUpdated")), // Fallback for compatibility
                commandSocketId = result.getString("commandSocketId"),
                updateSocketId = result.getString("updateSocketId"),
                reconnectable = result.getBoolean("reconnectable", true) // Default to true for backward compatibility
            )
        } catch (e: Exception) {
            if (e is IllegalArgumentException) {
                // Just rethrow our own exception
                throw e
            }
            log.error("Error finding connection: {}", connectionId, e)
            throw IllegalArgumentException("Error finding connection: $connectionId", e)
        }
    }

    /**
     * Update the lastActivity timestamp
     * Improved error handling to properly propagate errors
     */
    override suspend fun updateLastActivity(connectionId: String): Connection? {
        if (!exists(connectionId)) {
            log.debug("Connection doesn't exist for updateLastActivity: {}", connectionId)
            return null
        }

        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject().put("lastActivity", now))

        try {
            val result = mongoClient.updateCollection(collection, query, update).await()
            if (result.docModified == 0L) {
                log.debug("Connection not found for updateLastActivity: {}", connectionId)
                return null
            }

            return find(connectionId)
        } catch (e: Exception) {
            log.error("Error updating activity timestamp: {}", connectionId, e)
            throw e
        }
    }

    /**
     * Update the reconnectable flag
     * Improved error handling to properly propagate errors
     */
    override suspend fun updateReconnectable(connectionId: String, reconnectable: Boolean): Connection? {
        if (!exists(connectionId)) {
            log.debug("Connection doesn't exist for updateReconnectable: {}", connectionId)
            return null
        }

        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject()
                .put("lastUpdated", now)
                .put("lastActivity", now)
                .put("reconnectable", reconnectable)
            )

        try {
            val result = mongoClient.updateCollection(collection, query, update).await()
            if (result.docModified == 0L) {
                log.debug("Connection not found for updateReconnectable: {}", connectionId)
                return null
            }

            return find(connectionId)
        } catch (e: Exception) {
            log.error("Error updating reconnectable flag: {}", connectionId, e)
            throw e
        }
    }

    /**
     * Update the update socket ID
     * Improved error handling to throw exceptions instead of using fallbacks
     */
    override suspend fun updateUpdateSocketId(connectionId: String, updateSocketId: String?): Connection {
        // First check existence
        if (!exists(connectionId)) {
            log.error("Connection {} doesn't exist for updateUpdateSocketId", connectionId)
            throw IllegalArgumentException("Connection not found: $connectionId")
        }

        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject()
                .put("lastUpdated", now)
                .put("lastActivity", now)
                .put("updateSocketId", updateSocketId)
            )

        try {
            val result = mongoClient.updateCollection(collection, query, update).await()
            if (result.docModified == 0L) {
                log.error("Connection not found for updateUpdateSocketId: {}", connectionId)
                throw IllegalStateException("Failed to update connection: $connectionId")
            }

            return find(connectionId)
        } catch (e: Exception) {
            log.error("Error updating update socket ID: {}", connectionId, e)
            throw e
        }
    }

    /**
     * Update the socket IDs
     * Improved error handling to throw exceptions instead of using fallbacks
     */
    override suspend fun updateSocketIds(connectionId: String, commandSocketId: String?, updateSocketId: String?): Connection {
        // First check existence
        if (!exists(connectionId)) {
            log.error("Connection {} doesn't exist for updateSocketIds", connectionId)
            throw IllegalArgumentException("Connection not found: $connectionId")
        }

        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject()
                .put("lastUpdated", now)
                .put("lastActivity", now)
                .put("commandSocketId", commandSocketId)
                .put("updateSocketId", updateSocketId)
            )
        log.info("[TRACE] MongoDB update query: {} with update: {}", query.encode(), update.encode())

        try {
            val result = mongoClient.updateCollection(collection, query, update).await()
            if (result.docModified == 0L) {
                log.error("Connection not found for updateSocketIds: {}", connectionId)
                throw IllegalStateException("Failed to update connection: $connectionId")
            }

            return find(connectionId)
        } catch (e: Exception) {
            log.error("Error updating socket IDs: {}", connectionId, e)
            throw e
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
                    lastActivity = doc.getLong("lastActivity", doc.getLong("lastUpdated")),
                    commandSocketId = doc.getString("commandSocketId"),
                    updateSocketId = doc.getString("updateSocketId"),
                    reconnectable = doc.getBoolean("reconnectable", true)
                )
            }
        } catch (e: Exception) {
            log.error("Error finding connections with update socket", e)
            throw e
        }
    }

    /**
     * Find connections that haven't had activity for a specified time
     * @param olderThanMs maximum age in milliseconds
     */
    override suspend fun findInactiveConnections(olderThanMs: Long): List<Connection> {
        val cutoffTime = System.currentTimeMillis() - olderThanMs
        val query = JsonObject().put("lastActivity", JsonObject().put("\$lt", cutoffTime))

        try {
            val documents = mongoClient.find(collection, query).await()
            return documents.map { doc ->
                Connection(
                    _id = doc.getString("_id"),
                    createdAt = doc.getLong("createdAt"),
                    lastUpdated = doc.getLong("lastUpdated"),
                    lastActivity = doc.getLong("lastActivity", doc.getLong("lastUpdated")),
                    commandSocketId = doc.getString("commandSocketId"),
                    updateSocketId = doc.getString("updateSocketId"),
                    reconnectable = doc.getBoolean("reconnectable", true)
                )
            }
        } catch (e: Exception) {
            log.error("Error finding inactive connections", e)
            throw e
        }
    }
}