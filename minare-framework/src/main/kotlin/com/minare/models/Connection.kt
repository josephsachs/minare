package com.minare.core.models

/**
 * Represents a client connection with two communication sockets:
 * - An up socket for receiving client requests
 * - An down socket for pushing updates to the client
 */
data class Connection(
    val _id: String,
    val createdAt: Long,
    val lastUpdated: Long,
    val lastActivity: Long = System.currentTimeMillis(),
    val upSocketId: String? = null,
    val upSocketDeploymentId: String? = null,
    val downSocketId: String? = null,
    val downSocketDeploymentId: String? = null,
    val reconnectable: Boolean = true
)