package com.minare.core.storage.interfaces

import com.minare.core.transport.models.Connection

interface ConnectionStore {
    suspend fun create(): Connection
    suspend fun delete(connectionId: String)
    suspend fun find(connectionId: String): Connection
    suspend fun find(connectionId: Set<String>): Set<Connection>
    suspend fun putDownSocket(connectionId: String, socketId: String?, instanceId: String?): Connection
    suspend fun putUpSocket(connectionId: String, socketId: String?, instanceId: String?): Connection
    suspend fun findAllWithDownSocket(): List<Connection>
    suspend fun updateLastActivity(connectionId: String): Connection?
    suspend fun updateReconnectable(connectionId: String, reconnectable: Boolean): Connection?
    suspend fun exists(connectionId: String): Boolean
    suspend fun findInactiveConnections(olderThanMs: Long): List<Connection>
}