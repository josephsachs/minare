package com.minare.core.models

/**
 * Represents a client connection with two communication sockets:
 * - A command socket for receiving client requests
 * - An update socket for pushing updates to the client
 */
data class Connection(
    val id: String,
    val createdAt: Long,
    val lastUpdated: Long,
    val commandSocketId: String? = null,
    val updateSocketId: String? = null
)