package com.minare.core.storage.interfaces

import com.minare.core.transport.models.Connection

interface ConnectionStore {
    suspend fun create(): Connection
    suspend fun delete(connectionId: String)
    suspend fun find(connectionId: String): Connection
    suspend fun find(connectionId: Set<String>): Set<Connection>
    suspend fun putDownSocket(connectionId: String, socketId: String?, deploymentId: String?): Connection
    suspend fun putUpSocket(connectionId: String, socketId: String?, deploymentId: String?): Connection
    suspend fun findAllWithDownSocket(): List<Connection>

    /**
     * Update the lastActivity timestamp for a connection
     */
    suspend fun updateLastActivity(connectionId: String): Connection?

    /**
     * Update the reconnectable flag for a connection
     */
    suspend fun updateReconnectable(connectionId: String, reconnectable: Boolean): Connection?

    /**
     * Check if a connection exists by ID
     */
    suspend fun exists(connectionId: String): Boolean

    /**
     * Find connections that haven't had activity for a specified time
     */
    suspend fun findInactiveConnections(olderThanMs: Long): List<Connection>
}

enum class SocketType {
    Up,
    Down
}