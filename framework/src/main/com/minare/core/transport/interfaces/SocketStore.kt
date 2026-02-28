package com.minare.core.transport.interfaces

interface SocketStore<T> {
    fun put(connectionId: String, socket: T)
    fun getConnectionId(socket: T): String?
    fun get(connectionId: String): T?
    fun remove(connectionId: String): T?
    fun remove(socket: T): Boolean
    fun count(): Int
    fun send(connectionId: String, message: String): Boolean
    fun close(connectionId: String): Boolean
    fun allConnectionIds(): Set<String>
}