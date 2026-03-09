package com.minare.core.transport.models

import java.io.Serializable

/**
 * Represents a client connection with two communication sockets:
 * - An up socket for receiving client requests
 * - An down socket for pushing updates to the client
 *
 * The optional [meta] map carries application-defined key-value pairs
 * supplied by the client at connection time (e.g. `enable_metrics`).
 * It is immutable once the connection is created.
 */
data class Connection(
    val id: String,
    val createdAt: Long,
    val lastUpdated: Long,
    val lastActivity: Long = System.currentTimeMillis(),
    val upSocketId: String? = null,
    val upSocketInstanceId: String? = null,
    val downSocketId: String? = null,
    val downSocketInstanceId: String? = null,
    val reconnectable: Boolean = true,
    val meta: Map<String, String>? = null
): Serializable