package com.minare.core.storage.adapters

import com.google.inject.Singleton
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.models.Connection
import io.vertx.core.json.JsonObject
import io.vertx.ext.mongo.MongoClient
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory
import java.util.UUID
import com.google.inject.Inject

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
            id = connectionId,
            createdAt = now,
            lastUpdated = now,
            lastActivity = now,
            upSocketId = null,
            upSocketInstanceId = null,
            downSocketId = null,
            downSocketInstanceId = null,
            reconnectable = true
        )

        val document = JsonObject()
            .put("_id", connection.id)
            .put("createdAt", connection.createdAt)
            .put("lastUpdated", connection.lastUpdated)
            .put("lastActivity", connection.lastActivity)
            .put("upSocketId", connection.upSocketId)
            .put("upSocketDeploymentId", connection.upSocketInstanceId)
            .put("downSocketId", connection.downSocketId)
            .put("downSocketDeploymentId", connection.downSocketInstanceId)
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
        try {
            val query = JsonObject().put("_id", connectionId)
            val result = mongoClient.removeDocuments(collection, query).await()

            if (result.removedCount == 0L) {
                log.debug("No connection found to delete: {}", connectionId)
            } else {
                log.info("Connection deleted: {}", connectionId)
            }
        } catch (e: Exception) {
            log.error("Failed to delete connection: {}", connectionId, e)
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
                id = result.getString("_id"),
                createdAt = result.getLong("createdAt"),
                lastUpdated = result.getLong("lastUpdated"),
                lastActivity = result.getLong("lastActivity", result.getLong("lastUpdated")), // Fallback for compatibility
                upSocketId = result.getString("upSocketId"),
                upSocketInstanceId = result.getString("upSocketDeploymentId"),
                downSocketId = result.getString("downSocketId"),
                downSocketInstanceId = result.getString("downSocketDeploymentId"),
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
     * Find multiple connections by their IDs
     * Returns a set containing all found connections
     * Note: If any connection ID doesn't exist, it will be omitted from the results
     * without throwing an exception
     *
     * @param connectionId Set of connection IDs to find
     * @return Set of found Connection objects
     */
    override suspend fun find(connectionId: Set<String>): Set<Connection> {
        if (connectionId.isEmpty()) {
            return emptySet()
        }

        val query = JsonObject().put("_id", JsonObject().put("\$in", connectionId.toList()))

        try {
            val documents = mongoClient.find(collection, query).await()

            return documents.map { doc ->
                Connection(
                    id = doc.getString("_id"),
                    createdAt = doc.getLong("createdAt"),
                    lastUpdated = doc.getLong("lastUpdated"),
                    lastActivity = doc.getLong("lastActivity", doc.getLong("lastUpdated")), // Fallback for compatibility
                    upSocketId = doc.getString("upSocketId"),
                    upSocketInstanceId = doc.getString("upSocketDeploymentId"),
                    downSocketId = doc.getString("downSocketId"),
                    downSocketInstanceId = doc.getString("downSocketDeploymentId"),
                    reconnectable = doc.getBoolean("reconnectable", true) // Default to true for backward compatibility
                )
            }.toSet()
        } catch (e: Exception) {
            log.error("Error finding multiple connections: {}", connectionId, e)
            throw IllegalArgumentException("Error finding multiple connections", e)
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
     * Update the down socket ID
     * Improved error handling to throw exceptions instead of using fallbacks
     */
    override suspend fun putDownSocket(connectionId: String, socketId: String?, deploymentId: String?): Connection {
        if (!exists(connectionId)) {
            log.error("Connection {} doesn't exist for updateDownSocketId", connectionId)
            throw IllegalArgumentException("Connection not found: $connectionId")
        }

        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject()
                .put("lastUpdated", now)
                .put("lastActivity", now)
                .put("downSocketId", socketId)
                .put("downSocketDeploymentId", deploymentId)
            )

        try {
            val result = mongoClient.updateCollection(collection, query, update).await()
            if (result.docModified == 0L) {
                log.error("Connection not found for updateUpdateSocketId: {}", connectionId)
                throw IllegalStateException("Failed to update connection: $connectionId")
            }

            return find(connectionId)
        } catch (e: Exception) {
            log.error("Error updating down socket ID: {}", connectionId, e)
            throw e
        }
    }

    /**
     * Update the up socket ID
     * Improved error handling to throw exceptions instead of using fallbacks
     */
    override suspend fun putUpSocket(connectionId: String, socketId: String?, deploymentId: String?): Connection {
        // First check existence
        if (!exists(connectionId)) {
            log.error("Connection {} doesn't exist for putUpSocket", connectionId)
            throw IllegalArgumentException("Connection not found: $connectionId")
        }

        val query = JsonObject().put("_id", connectionId)
        val now = System.currentTimeMillis()
        val update = JsonObject()
            .put("\$set", JsonObject()
                .put("lastUpdated", now)
                .put("lastActivity", now)
                .put("upSocketId", socketId)
                .put("upSocketDeploymentId", deploymentId)
            )

        try {
            val result = mongoClient.updateCollection(collection, query, update).await()
            if (result.docModified == 0L) {
                log.error("Connection not found for putUpSocket: {}", connectionId)
                throw IllegalStateException("Failed to update connection: $connectionId")
            }

            return find(connectionId)
        } catch (e: Exception) {
            log.error("Error updating up socket ID: {}", connectionId, e)
            throw e
        }
    }

    /**
     * Get all connections with down sockets
     */
    override suspend fun findAllWithDownSocket(): List<Connection> {
        val query = JsonObject().put("downSocketId", JsonObject().put("\$ne", null))

        try {
            val documents = mongoClient.find(collection, query).await()
            return documents.map { doc ->
                Connection(
                    id = doc.getString("_id"),
                    createdAt = doc.getLong("createdAt"),
                    lastUpdated = doc.getLong("lastUpdated"),
                    lastActivity = doc.getLong("lastActivity", doc.getLong("lastUpdated")),
                    upSocketId = doc.getString("upSocketId"),
                    upSocketInstanceId = doc.getString("upSocketDeploymentId"),
                    downSocketId = doc.getString("downSocketId"),
                    downSocketInstanceId = doc.getString("downSocketDeploymentId"),
                    reconnectable = doc.getBoolean("reconnectable", true)
                )
            }
        } catch (e: Exception) {
            log.error("Error finding connections with down socket", e)
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
                    id = doc.getString("_id"),
                    createdAt = doc.getLong("createdAt"),
                    lastUpdated = doc.getLong("lastUpdated"),
                    lastActivity = doc.getLong("lastActivity", doc.getLong("lastUpdated")),
                    upSocketId = doc.getString("upSocketId"),
                    upSocketInstanceId = doc.getString("upSocketDeploymentId"),
                    downSocketId = doc.getString("downSocketId"),
                    downSocketInstanceId = doc.getString("downSocketDeploymentId"),
                    reconnectable = doc.getBoolean("reconnectable", true)
                )
            }
        } catch (e: Exception) {
            log.error("Error finding inactive connections", e)
            throw e
        }
    }
}