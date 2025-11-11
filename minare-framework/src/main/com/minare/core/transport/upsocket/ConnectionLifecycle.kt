package com.minare.worker.upsocket

import com.google.inject.Inject
import com.google.inject.Singleton
import com.google.inject.name.Named
import com.minare.core.MinareApplication
import com.minare.cache.ConnectionCache
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.downsocket.services.ConnectionTracker
import com.minare.utils.HeartbeatManager
import com.minare.core.utils.vertx.VerticleLogger
import com.minare.utils.WebSocketUtils
import io.vertx.core.Vertx
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.core.json.JsonObject

/**
 * Handles socket lifecycle events including connection tracking and verticle caches
 */
@Singleton
class ConnectionLifecycle @Inject constructor(
    private val vertx: Vertx,
    private val vlog: VerticleLogger,
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache,
    private val channelStore: ChannelStore,
    private val connectionTracker: ConnectionTracker,
    private val heartbeatManager: HeartbeatManager
) {
    private val log = LoggerFactory.getLogger(ConnectionLifecycle::class.java)

    /**
     * Connect to the up socket
     */
    suspend fun initiateConnection(websocket: ServerWebSocket, deploymentId: String, traceId: String) {
        try {
            vlog.getEventLogger().logDbOperation("CREATE", "connections", emptyMap(), traceId)

            val startTime = System.currentTimeMillis()
            val connection = connectionStore.create()

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

            val upSocketId = "cs-${java.util.UUID.randomUUID()}"

            vlog.getEventLogger().logDbOperation(
                "UPDATE", "connections",
                mapOf("connectionId" to connection._id, "action" to "down_socket_ids"), traceId
            )

            val updatedConnection = connectionStore.putUpSocket(
                connection._id,
                upSocketId,
                deploymentId
            )

            // Save cache entry with the updated connection
            connectionCache.storeConnection(updatedConnection)
            connectionCache.storeUpSocket(connection._id, websocket, updatedConnection)

            vlog.getEventLogger().logWebSocketEvent(
                "SOCKET_REGISTERED", connection._id,
                mapOf("socketType" to "up", "socketId" to upSocketId), traceId
            )

            WebSocketUtils.sendConfirmation(websocket, "connection_confirm", connection._id)
            heartbeatManager.startHeartbeat(upSocketId, connection._id, websocket)

            vlog.getEventLogger().trace(
                "CONNECTION_ESTABLISHED",
                mapOf("connectionId" to connection._id), traceId
            )

            vertx.eventBus().publish(
                MinareApplication.ConnectionEvents.ADDRESS_UP_SOCKET_CONNECTED,
                JsonObject()
                    .put("connectionId", connection._id)
                    .put("traceId", traceId)
            )
        } catch (e: Exception) {
            vlog.getEventLogger().logError("CONNECTION_FAILED", e, emptyMap(), traceId)
            WebSocketUtils.sendErrorResponse(websocket, e, null, vlog)
        }
    }

    /**
     * Coordinated connection cleanup process
     */
    suspend fun cleanupConnection(connectionId: String): Boolean {
        val traceId = connectionTracker.getTraceId(connectionId)

        try {
            vlog.getEventLogger().trace(
                "CONNECTION_CLEANUP_STARTED", mapOf(
                    "connectionId" to connectionId
                ), traceId
            )

            // Clean up channels
            val channelCleanupResult = cleanupConnectionChannels(connectionId)
            if (!channelCleanupResult) {
                vlog.getEventLogger().trace(
                    "CHANNEL_CLEANUP_FAILED", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
            }

            // Clean up sockets and stop heartbeat
            heartbeatManager.stopHeartbeat(connectionId)
            val updateSocket = connectionCache.getDownSocket(connectionId)
            val socketCleanupResult = cleanupConnectionSockets(connectionId, updateSocket != null)
            if (!socketCleanupResult) {
                vlog.getEventLogger().trace(
                    "SOCKET_CLEANUP_FAILED", mapOf(
                        "connectionId" to connectionId
                    ), traceId
                )
            }

            // Mark connection as not reconnectable
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

            // Remove the connection from DB and cache
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
                connectionCache.removeUpSocket(connectionId)
                connectionCache.removeDownSocket(connectionId)
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
    suspend fun cleanupConnectionChannels(connectionId: String): Boolean {
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
                    val result = channelStore.removeChannelClient(channelId, connectionId)
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

            // Clean up up socket
            try {
                connectionCache.removeUpSocket(connectionId)?.let { socket ->
                    if (!socket.isClosed()) {
                        try {
                            socket.close()
                            vlog.getEventLogger().trace("UP_SOCKET_CLOSED", mapOf(
                                "connectionId" to connectionId,
                                "socketId" to socket.textHandlerID()
                            ), traceId)
                        } catch (e: Exception) {
                            vlog.logVerticleError("UP_SOCKET_CLOSE", e, mapOf(
                                "connectionId" to connectionId
                            ))
                        }
                    }
                }
            } catch (e: Exception) {
                vlog.logVerticleError("UP_SOCKET_CLEANUP", e, mapOf(
                    "connectionId" to connectionId
                ))
                success = false
            }

            // Clean up down socket if it exists
            if (hasUpdateSocket) {
                try {
                    connectionCache.removeDownSocket(connectionId)?.let { socket ->
                        if (!socket.isClosed()) {
                            try {
                                socket.close()
                                vlog.getEventLogger().trace("DOWN_SOCKET_CLOSED", mapOf(
                                    "connectionId" to connectionId,
                                    "socketId" to socket.textHandlerID()
                                ), traceId)
                            } catch (e: Exception) {
                                vlog.logVerticleError("DOWN_SOCKET_CLOSE", e, mapOf(
                                    "connectionId" to connectionId
                                ))
                            }
                        }
                    }
                } catch (e: Exception) {
                    vlog.logVerticleError("DOWN_SOCKET_CLEANUP", e, mapOf(
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