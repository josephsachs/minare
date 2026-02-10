package com.minare.core.storage.adapters

import com.google.inject.Singleton
import com.hazelcast.core.HazelcastInstance
import com.hazelcast.map.IMap
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.models.Connection
import org.slf4j.LoggerFactory
import java.util.UUID
import com.google.inject.Inject
import com.minare.exceptions.ClientConnectionException

@Singleton
class HazelcastConnectionStore @Inject constructor(
    private val hazelcast: HazelcastInstance
) : ConnectionStore {
    private val log = LoggerFactory.getLogger(HazelcastConnectionStore::class.java)
    private val map: IMap<String, Connection> by lazy { hazelcast.getMap("connections") }

    override suspend fun create(): Connection {
        val connectionId = UUID.randomUUID().toString()
        val now = System.currentTimeMillis()

        val connection = Connection(
            _id = connectionId,
            createdAt = now,
            lastUpdated = now,
            lastActivity = now,
            upSocketId = null,
            upSocketDeploymentId = null,
            downSocketId = null,
            downSocketDeploymentId = null,
            reconnectable = true
        )

        map[connectionId] = connection
        log.info("Connection created: {}", connectionId)
        return connection
    }

    override suspend fun delete(connectionId: String) {
        val removed = map.remove(connectionId)
        if (removed == null) {
            log.debug("No connection found to delete: {}", connectionId)
        } else {
            log.info("Connection deleted: {}", connectionId)
        }
    }

    override suspend fun exists(connectionId: String): Boolean {
        return map.containsKey(connectionId)
    }

    override suspend fun find(connectionId: String): Connection {
        return map[connectionId]
            ?: throw ClientConnectionException("Connection not found: $connectionId")
    }

    override suspend fun find(connectionId: Set<String>): Set<Connection> {
        if (connectionId.isEmpty()) {
            return emptySet()
        }
        return map.getAll(connectionId).values.toSet()
    }

    override suspend fun updateLastActivity(connectionId: String): Connection? {
        val existing = map[connectionId] ?: return null
        val updated = existing.copy(lastActivity = System.currentTimeMillis())
        map[connectionId] = updated
        return updated
    }

    override suspend fun updateReconnectable(connectionId: String, reconnectable: Boolean): Connection? {
        val existing = map[connectionId] ?: return null
        val now = System.currentTimeMillis()
        val updated = existing.copy(
            lastUpdated = now,
            lastActivity = now,
            reconnectable = reconnectable
        )
        map[connectionId] = updated
        return updated
    }

    override suspend fun putDownSocket(connectionId: String, socketId: String?, deploymentId: String?): Connection {
        val existing = map[connectionId]
            ?: throw IllegalArgumentException("Connection not found: $connectionId")

        val now = System.currentTimeMillis()
        val updated = existing.copy(
            lastUpdated = now,
            lastActivity = now,
            downSocketId = socketId,
            downSocketDeploymentId = deploymentId
        )
        map[connectionId] = updated
        return updated
    }

    override suspend fun putUpSocket(connectionId: String, socketId: String?, deploymentId: String?): Connection {
        val existing = map[connectionId]
            ?: throw IllegalArgumentException("Connection not found: $connectionId")

        val now = System.currentTimeMillis()
        val updated = existing.copy(
            lastUpdated = now,
            lastActivity = now,
            upSocketId = socketId,
            upSocketDeploymentId = deploymentId
        )
        map[connectionId] = updated
        return updated
    }

    override suspend fun findAllWithDownSocket(): List<Connection> {
        return map.values.filter { it.downSocketId != null }
    }

    override suspend fun findInactiveConnections(olderThanMs: Long): List<Connection> {
        val cutoffTime = System.currentTimeMillis() - olderThanMs
        return map.values.filter { it.lastActivity < cutoffTime }
    }
}