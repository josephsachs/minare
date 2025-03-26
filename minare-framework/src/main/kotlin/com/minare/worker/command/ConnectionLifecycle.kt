package com.minare.worker.command

import com.google.inject.Inject
import com.minare.cache.ConnectionCache
import com.minare.persistence.ChannelStore
import com.minare.persistence.ConnectionStore
import com.minare.utils.ConnectionTracker
import com.minare.utils.HeartbeatManager
import com.minare.utils.VerticleLogger
import com.minare.utils.WebSocketUtils
import io.vertx.core.http.ServerWebSocket

class ConnectionLifecycle @Inject constructor(
    private val vlog: VerticleLogger,
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache,
    private val channelStore: ChannelStore,
    private val connectionTracker: ConnectionTracker,
    private val heartbeatManager: HeartbeatManager
) {
    /**
     * Connect to the command socket
     */
    public suspend fun initiateConnection(websocket: ServerWebSocket, traceId: String) {
        try {
            vlog.getEventLogger().logDbOperation("CREATE", "connections", emptyMap(), traceId)

            val startTime = System.currentTimeMillis()
            val connection = connectionStore.create()

            // Log performance metrics
            val createTime = System.currentTimeMillis() - startTime
            vlog.getEventLogger().logPerformance(
                "CREATE_CONNECTION", createTime,
                mapOf("connectionId" to connection._id), traceId
            )

            connectionCache.storeConnection(connection)
            connectionTracker.registerConnection(connection._id, traceId, websocket)

            vlog.getEventLogger().logStateChange(
                "Connection", "NONE", "CREATED",
                mapOf("connectionId" to connection._id), traceId
            )

            // Generate a command socket ID
            val commandSocketId = "cs-${java.util.UUID.randomUUID()}"

            vlog.getEventLogger().logDbOperation(
                "UPDATE", "connections",
                mapOf("connectionId" to connection._id, "action" to "update_socket_ids"), traceId
            )

            val updatedConnection = connectionStore.updateSocketIds(
                connection._id,
                commandSocketId,
                null
            )

            // IMPORTANT: Update the cache with the updated connection
            connectionCache.storeConnection(updatedConnection)
            connectionCache.storeCommandSocket(connection._id, websocket, updatedConnection)

            vlog.getEventLogger().logWebSocketEvent(
                "SOCKET_REGISTERED", connection._id,
                mapOf("socketType" to "command", "socketId" to commandSocketId), traceId
            )

            WebSocketUtils.sendConfirmation(websocket, "connection_confirm", connection._id)
            heartbeatManager.startHeartbeat(connection._id, websocket)

            vlog.getEventLogger().trace(
                "CONNECTION_ESTABLISHED",
                mapOf("connectionId" to connection._id), traceId
            )
        } catch (e: Exception) {
            vlog.getEventLogger().logError("CONNECTION_FAILED", e, emptyMap(), traceId)
            WebSocketUtils.sendErrorResponse(websocket, e, null, vlog)
        }
    }

    /**
     * Coordinated connection cleanup process
     */
    public suspend fun cleanupConnection(connectionId: String): Boolean {
        val traceId = connectionTracker.getTraceId(connectionId)

        try {
            vlog.getEventLogger().trace(
                "CONNECTION_CLEANUP_STARTED", mapOf(
                    "connectionId" to connectionId
                ), traceId
            )

            // Step 1: Clean up channels
            val channelCleanupResult = cleanupConnectionChannels(connectionId)
            if (!channelCleanupResult) {
                vlog.getEventLogger().trace(
                    "CHANNEL_CLEANUP_FAILED", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
            }

            // Step 2: Clean up sockets and stop heartbeat
            heartbeatManager.stopHeartbeat(connectionId)
            val updateSocket = connectionCache.getUpdateSocket(connectionId)
            val socketCleanupResult = cleanupConnectionSockets(connectionId, updateSocket != null)
            if (!socketCleanupResult) {
                vlog.getEventLogger().trace(
                    "SOCKET_CLEANUP_FAILED", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
            }

            // Step 3: Mark connection as not reconnectable
            try {
                connectionStore.updateReconnectable(connectionId, false)
                vlog.getEventLogger().logStateChange(
                    "Connection", "DISCONNECTED", "NOT_RECONNECTABLE",
                    mapOf("connectionId" to connectionId), traceId
                )
            } catch (e: Exception) {
                vlog.logVerticleError(
                    "SET_NOT_RECONNECTABLE", e, mapOf(
                        "connectionId" to connectionId
                    )
                )
            }

            // Step 4: Remove the connection from DB and cache
            try {
                // Try to delete from the database first
                connectionStore.delete(connectionId)
                vlog.getEventLogger().logDbOperation(
                    "DELETE", "connections",
                    mapOf("connectionId" to connectionId), traceId
                )
            } catch (e: Exception) {
                vlog.logVerticleError(
                    "DB_CONNECTION_DELETE", e, mapOf(
                        "connectionId" to connectionId
                    )
                )
                // Continue anyway - the connection might already be gone
            }

            // Final cleanup from cache
            try {
                connectionCache.removeConnection(connectionId)
                connectionTracker.removeConnection(connectionId)
                vlog.getEventLogger().trace(
                    "CACHE_CONNECTION_REMOVED", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
            } catch (e: Exception) {
                vlog.logVerticleError(
                    "CACHE_CONNECTION_REMOVE", e, mapOf(
                        "connectionId" to connectionId
                    )
                )
            }

            vlog.getEventLogger().trace(
                "CONNECTION_CLEANUP_COMPLETED", mapOf(
                    "connectionId" to connectionId
                ), traceId
            )

            return true
        } catch (e: Exception) {
            vlog.logVerticleError(
                "CONNECTION_CLEANUP", e, mapOf(
                    "connectionId" to connectionId
                )
            )

            // Do emergency cleanup as a last resort
            try {
                connectionCache.removeCommandSocket(connectionId)
                connectionCache.removeUpdateSocket(connectionId)
                connectionCache.removeConnection(connectionId)
                connectionTracker.removeConnection(connectionId)
                vlog.getEventLogger().trace(
                    "EMERGENCY_CLEANUP_COMPLETED", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
            } catch (innerEx: Exception) {
                vlog.logVerticleError(
                    "EMERGENCY_CLEANUP", innerEx, mapOf(
                        "connectionId" to connectionId
                    )
                )
            }

            return false
        }
    }

    /**
     * Cleans up channel memberships for a connection
     */
    public suspend fun cleanupConnectionChannels(connectionId: String): Boolean {
        val traceId = connectionTracker.getTraceId(connectionId)

        try {
            // Get the channels this connection is in
            val channels = channelStore.getChannelsForClient(connectionId)

            vlog.getEventLogger().trace("CHANNEL_CLEANUP_STARTED", mapOf(
                "connectionId" to connectionId,
                "channelCount" to channels.size
            ), traceId)

            if (channels.isEmpty()) {
                vlog.getEventLogger().trace("NO_CHANNELS_FOUND", mapOf(
                    "connectionId" to connectionId
                ), traceId)
                return true
            }

            // Remove the connection from each channel
            var success = true
            for (channelId in channels) {
                try {
                    val result = channelStore.removeClientFromChannel(channelId, connectionId)
                    if (!result) {
                        vlog.getEventLogger().trace("CHANNEL_REMOVAL_FAILED", mapOf(
                            "connectionId" to connectionId,
                            "channelId" to channelId
                        ), traceId)
                        success = false
                    } else {
                        vlog.getEventLogger().trace("CHANNEL_REMOVAL_SUCCEEDED", mapOf(
                            "connectionId" to connectionId,
                            "channelId" to channelId
                        ), traceId)
                    }
                } catch (e: Exception) {
                    vlog.logVerticleError("CHANNEL_REMOVAL", e, mapOf(
                        "connectionId" to connectionId,
                        "channelId" to channelId
                    ))
                    success = false
                }
            }

            vlog.getEventLogger().trace("CHANNEL_CLEANUP_COMPLETED", mapOf(
                "connectionId" to connectionId,
                "success" to success
            ), traceId)

            return success
        } catch (e: Exception) {
            vlog.logVerticleError("CHANNEL_CLEANUP", e, mapOf(
                "connectionId" to connectionId
            ))
            return false
        }
    }

    /**
     * Cleans up sockets for a connection
     */
    public suspend fun cleanupConnectionSockets(connectionId: String, hasUpdateSocket: Boolean): Boolean {
        val traceId = connectionTracker.getTraceId(connectionId)

        try {
            var success = true

            vlog.getEventLogger().trace("SOCKET_CLEANUP_STARTED", mapOf(
                "connectionId" to connectionId,
                "hasUpdateSocket" to hasUpdateSocket
            ), traceId)

            // Clean up command socket
            try {
                connectionCache.removeCommandSocket(connectionId)?.let { socket ->
                    if (!socket.isClosed()) {
                        try {
                            socket.close()
                            vlog.getEventLogger().trace("COMMAND_SOCKET_CLOSED", mapOf(
                                "connectionId" to connectionId,
                                "socketId" to socket.textHandlerID()
                            ), traceId)
                        } catch (e: Exception) {
                            vlog.logVerticleError("COMMAND_SOCKET_CLOSE", e, mapOf(
                                "connectionId" to connectionId
                            ))
                        }
                    }
                }
            } catch (e: Exception) {
                vlog.logVerticleError("COMMAND_SOCKET_CLEANUP", e, mapOf(
                    "connectionId" to connectionId
                ))
                success = false
            }

            // Clean up update socket if it exists
            if (hasUpdateSocket) {
                try {
                    connectionCache.removeUpdateSocket(connectionId)?.let { socket ->
                        if (!socket.isClosed()) {
                            try {
                                socket.close()
                                vlog.getEventLogger().trace("UPDATE_SOCKET_CLOSED", mapOf(
                                    "connectionId" to connectionId,
                                    "socketId" to socket.textHandlerID()
                                ), traceId)
                            } catch (e: Exception) {
                                vlog.logVerticleError("UPDATE_SOCKET_CLOSE", e, mapOf(
                                    "connectionId" to connectionId
                                ))
                            }
                        }
                    }
                } catch (e: Exception) {
                    vlog.logVerticleError("UPDATE_SOCKET_CLEANUP", e, mapOf(
                        "connectionId" to connectionId
                    ))
                    success = false
                }
            }

            vlog.getEventLogger().trace("SOCKET_CLEANUP_COMPLETED", mapOf(
                "connectionId" to connectionId,
                "success" to success
            ), traceId)

            return success
        } catch (e: Exception) {
            vlog.logVerticleError("SOCKET_CLEANUP", e, mapOf(
                "connectionId" to connectionId
            ))
            return false
        }
    }
}