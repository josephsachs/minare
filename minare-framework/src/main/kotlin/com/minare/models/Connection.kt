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
    val lastActivity: Long = System.currentTimeMillis(),
    val commandSocketId: String? = null,
    val commandDeploymentId: String? = null,
    val updateSocketId: String? = null,
    val updateDeploymentId: String? = null,
    val reconnectable: Boolean = true
)