package com.minare.persistence

import com.minare.core.models.Connection

interface ConnectionStore {
    suspend fun create(): Connection
    suspend fun delete(connectionId: String)
    suspend fun find(connectionId: String): Connection
    suspend fun updateUpdateSocketId(connectionId: String, updateSocketId: String?): Connection
    suspend fun updateSocketIds(connectionId: String, commandSocketId: String?, updateSocketId: String?): Connection
    suspend fun findAllWithUpdateSocket(): List<Connection>
}