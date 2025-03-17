package com.minare.persistence

import com.minare.core.models.Connection
import io.vertx.core.Future

interface ConnectionStore {
    fun create(): Future<Connection>
    fun delete(connectionId: String): Future<Nothing?>
    fun find(connectionId: String): Future<Connection>
    fun updateUpdateSocketId(connectionId: String, updateSocketId: String?): Future<Connection>
    fun updateSocketIds(connectionId: String, commandSocketId: String?, updateSocketId: String?): Future<Connection>
    fun findAllWithUpdateSocket(): Future<List<Connection>>
}