package com.minare.core.models

/**
 * Represents a client connection with two communication sockets:
 * - A command socket for receiving client requests
 * - An update socket for pushing updates to the client
 */
data class Connection(
    val _id: String,
    val createdAt: Long,
    val lastUpdated: Long,
    val lastActivity: Long = System.currentTimeMillis(), // New field to track last activity
    val commandSocketId: String? = null,
    val updateSocketId: String? = null,
    val reconnectable: Boolean = true // Flag to control if connection can be reconnected
)