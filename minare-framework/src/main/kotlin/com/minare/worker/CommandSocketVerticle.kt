package com.minare.worker

import com.minare.cache.ConnectionCache
import com.minare.controller.ConnectionController
import com.minare.core.entity.ReflectionCache
import com.minare.core.websocket.CommandMessageHandler
import com.minare.persistence.ConnectionStore
import com.minare.persistence.ChannelStore
import com.minare.persistence.ContextStore
import com.minare.persistence.EntityStore
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * Verticle responsible for managing command socket connections and handling
 * socket lifecycle events.
 */
class CommandSocketVerticle @Inject constructor(
    private val connectionStore: ConnectionStore,
    private val connectionCache: ConnectionCache,
    private val connectionController: ConnectionController,
    private val channelStore: ChannelStore,
    private val contextStore: ContextStore,
    private val messageHandler: CommandMessageHandler,
    private val reflectionCache: ReflectionCache,
    private val entityStore: EntityStore
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(CommandSocketVerticle::class.java)

    // Map to store heartbeat timer IDs
    private val heartbeatTimers = mutableMapOf<String, Long>()

    companion object {
        const val ADDRESS_COMMAND_SOCKET_HANDLE = "minare.command.socket.handle"
        const val ADDRESS_COMMAND_SOCKET_CLOSE = "minare.command.socket.close"
        const val ADDRESS_CONNECTION_CLEANUP = "minare.connection.cleanup"
        const val ADDRESS_CHANNEL_CLEANUP = "minare.channel.cleanup"
        const val ADDRESS_SOCKET_CLEANUP = "minare.socket.cleanup"
        const val ADDRESS_ENTITY_SYNC = "minare.entity.sync"

        // Extended handshake timeout from 500ms to 3000ms (3 seconds)
        const val HANDSHAKE_TIMEOUT_MS = 3000L

        // Heartbeat interval of 15 seconds
        const val HEARTBEAT_INTERVAL_MS = 15000L
    }

    override suspend fun start() {
        log.info("Starting CommandSocketVerticle")

        // Register event bus consumers for socket events
        vertx.eventBus().consumer<JsonObject>(ADDRESS_COMMAND_SOCKET_HANDLE) { message ->
            val socketId = message.body().getString("socketId")
            log.debug("Received command socket handle request for socket: {}", socketId)

            // This is a placeholder - we'll integrate with actual socket handling later
            message.reply(JsonObject().put("success", true))
        }

        // Register entity sync handler
        vertx.eventBus().consumer<JsonObject>(ADDRESS_ENTITY_SYNC) { message ->
            val connectionId = message.body().getString("connectionId")
            val entityId = message.body().getString("entityId")

            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    log.debug("Handling entity sync request for entity {} from connection {}", entityId, connectionId)
                    val result = handleEntitySync(connectionId, entityId)
                    message.reply(JsonObject().put("success", result))
                } catch (e: Exception) {
                    log.error("Error handling entity sync", e)
                    message.reply(JsonObject().put("success", false).put("error", e.message))
                }
            }
        }

        // Register cleanup consumers
        vertx.eventBus().consumer<JsonObject>(ADDRESS_CONNECTION_CLEANUP) { message ->
            val connectionId = message.body().getString("connectionId")
            log.debug("Received connection cleanup request for: {}", connectionId)

            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val result = cleanupConnection(connectionId)
                    message.reply(JsonObject().put("success", result))
                } catch (e: Exception) {
                    log.error("Error during connection cleanup", e)
                    message.reply(JsonObject().put("success", false).put("error", e.message))
                }
            }
        }

        vertx.eventBus().consumer<JsonObject>(ADDRESS_CHANNEL_CLEANUP) { message ->
            val connectionId = message.body().getString("connectionId")
            log.debug("Received channel cleanup request for connection: {}", connectionId)

            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val result = cleanupConnectionChannels(connectionId)
                    message.reply(JsonObject().put("success", result))
                } catch (e: Exception) {
                    log.error("Error during channel cleanup", e)
                    message.reply(JsonObject().put("success", false).put("error", e.message))
                }
            }
        }

        vertx.eventBus().consumer<JsonObject>(ADDRESS_SOCKET_CLEANUP) { message ->
            val connectionId = message.body().getString("connectionId")
            val hasUpdateSocket = message.body().getBoolean("hasUpdateSocket", false)
            log.debug("Received socket cleanup request for connection: {}", connectionId)

            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val result = cleanupConnectionSockets(connectionId, hasUpdateSocket)
                    message.reply(JsonObject().put("success", result))
                } catch (e: Exception) {
                    log.error("Error during socket cleanup", e)
                    message.reply(JsonObject().put("success", false).put("error", e.message))
                }
            }
        }

        log.info("CommandSocketVerticle started successfully")
    }

    /**
     * Handle a new command socket connection
     * Enhanced to support reconnection of existing connections
     */
    suspend fun handleCommandSocket(websocket: ServerWebSocket) {
        log.info("New command socket connection from {}", websocket.remoteAddress())

        // Set up message handler for connection initialization or reconnect
        var handshakeCompleted = false

        websocket.textMessageHandler { message ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    // Only process the first message before handshake completes
                    if (!handshakeCompleted) {
                        val msg = JsonObject(message)
                        if (msg.containsKey("reconnect") && msg.containsKey("connectionId")) {
                            // This is a reconnection attempt
                            val connectionId = msg.getString("connectionId")
                            handshakeCompleted = true
                            handleReconnection(websocket, connectionId)
                        } else {
                            // For regular messages after handshake, use the standard handler
                            val msg = JsonObject(message)
                            handleMessage(websocket, msg)
                        }
                    } else {
                        // Regular message handling after handshake is complete
                        val msg = JsonObject(message)
                        handleMessage(websocket, msg)
                    }
                } catch (e: Exception) {
                    handleError(websocket, e)
                }
            }
        }

        websocket.closeHandler {
            CoroutineScope(vertx.dispatcher()).launch {
                handleClose(websocket)
            }
        }

        websocket.accept()
        log.info("[TRACE] Command socket accepted from {} with socketId: {}",
            websocket.remoteAddress(),
            websocket.textHandlerID())

        // If no reconnection message is received, create a new connection
        // Extended handshake timer to account for network latency
        vertx.setTimer(HANDSHAKE_TIMEOUT_MS) {
            if (!handshakeCompleted) {
                log.info("Handshake timer expired after {}ms, initiating new connection for socket from {}",
                    HANDSHAKE_TIMEOUT_MS, websocket.remoteAddress())
                handshakeCompleted = true
                CoroutineScope(vertx.dispatcher()).launch {
                    try {
                        initiateConnection(websocket)
                    } catch (e: Exception) {
                        log.error("Failed to initiate new connection after handshake timeout", e)
                        handleError(websocket, e)
                    }
                }
            }
        }
    }

    /**
     * Handle a reconnection attempt
     */
    private suspend fun handleReconnection(websocket: ServerWebSocket, connectionId: String) {
        try {
            log.info("Handling reconnection attempt for connection: {}", connectionId)

            // Check if connection exists and is reconnectable
            val exists = connectionStore.exists(connectionId)
            if (!exists) {
                log.warn("Reconnection failed: Connection {} does not exist", connectionId)
                sendReconnectionResponse(websocket, false, "Connection not found")
                initiateConnection(websocket)  // Fallback to creating a new connection
                return
            }

            // Find the connection to check reconnectable flag
            val connection = connectionStore.find(connectionId)

            if (!connection.reconnectable) {
                log.warn("Reconnection rejected: Connection {} is not reconnectable", connectionId)
                sendReconnectionResponse(websocket, false, "Connection not reconnectable")
                initiateConnection(websocket)  // Fallback to creating a new connection
                return
            }

            // Check last activity timestamp to see if within reconnection window
            val now = System.currentTimeMillis()
            val inactiveMs = now - connection.lastActivity
            val reconnectWindowMs = CleanupVerticle.CONNECTION_RECONNECT_WINDOW_MS

            if (inactiveMs > reconnectWindowMs) {
                log.warn(
                    "Reconnection window expired for connection {}: inactivity {}, window {}",
                    connectionId, inactiveMs, reconnectWindowMs
                )
                sendReconnectionResponse(websocket, false, "Reconnection window expired")
                initiateConnection(websocket)  // Fallback to creating a new connection
                return
            }

            // Close existing command socket if any
            connectionCache.getCommandSocket(connectionId)?.let { existingSocket ->
                try {
                    if (!existingSocket.isClosed()) {
                        existingSocket.close()
                        log.info("Closed existing command socket for connection {}", connectionId)
                    }
                } catch (e: Exception) {
                    log.warn("Error closing existing command socket for {}", connectionId, e)
                }
            }

            // Associate the new websocket with this connection
            connectionCache.storeCommandSocket(connectionId, websocket, connection)

            // Update the connection record with new socket ID and activity time
            val socketId = "cs-${java.util.UUID.randomUUID()}"
            val updatedConnection = connectionStore.updateSocketIds(
                connectionId,
                socketId,
                connection.updateSocketId
            )

            log.info("Reconnection successful for connection {}", connectionId)

            // Send confirmation to the client
            sendReconnectionResponse(websocket, true, null)

            // Start heartbeat for this connection
            startHeartbeat(connectionId)

            // If connection has both sockets again, mark as fully connected
            if (connectionCache.isFullyConnected(connectionId)) {
                connectionController.onClientFullyConnected(updatedConnection)
            }

        } catch (e: Exception) {
            log.error("Error handling reconnection for connection {}", connectionId, e)
            sendReconnectionResponse(websocket, false, "Internal error")
            initiateConnection(websocket)  // Fallback to creating a new connection
        }
    }

    /**
     * Send a reconnection response to the client
     */
    private fun sendReconnectionResponse(websocket: ServerWebSocket, success: Boolean, errorMessage: String?) {
        val response = JsonObject()
            .put("type", "reconnect_response")
            .put("success", success)
            .put("timestamp", System.currentTimeMillis())

        if (!success && errorMessage != null) {
            response.put("error", errorMessage)
        }

        websocket.writeTextMessage(response.encode())
    }

    /**
     * Connect to the command socket
     */
    private suspend fun initiateConnection(websocket: ServerWebSocket) {
        try {
            val connection = connectionStore.create()

            // Store in cache - separate from the DB operation
            connectionCache.storeConnection(connection)

            log.info("[TRACE] Connection cache state after storing new connection:")
            val cachedConnection = connectionCache.getConnection(connection._id)
            log.info("[TRACE] Cache contains connection {}: {}, commandSocketId={}",
                connection._id, cachedConnection != null, cachedConnection?.commandSocketId)

            // Generate a command socket ID
            val commandSocketId = "cs-${java.util.UUID.randomUUID()}"
            log.info("[TRACE] Setting command socket ID: {} for connection: {}", commandSocketId, connection._id)

            // Update the connection in the database
            val updatedConnection = connectionStore.updateSocketIds(
                connection._id,
                commandSocketId,
                null
            )

            // IMPORTANT: Update the cache with the updated connection to prevent cache/DB inconsistency
            connectionCache.storeConnection(updatedConnection)

            // Register command socket - now with the updated connection that has the command socket ID
            connectionCache.storeCommandSocket(connection._id, websocket, updatedConnection)

            log.info("[TRACE] Command socket DB update result: commandSocketId={}", updatedConnection.commandSocketId)

            sendConnectionConfirmation(websocket, connection._id)

            // Start heartbeat for this connection
            startHeartbeat(connection._id)
        } catch (e: Exception) {
            handleError(websocket, e)
        }
    }

    private fun sendConnectionConfirmation(websocket: ServerWebSocket, connectionId: String) {
        val confirmation = JsonObject()
            .put("type", "connection_confirm")
            .put("connectionId", connectionId)
            .put("timestamp", System.currentTimeMillis())

        websocket.writeTextMessage(confirmation.encode())
        log.info("Command socket established for {}", connectionId)
    }

    private suspend fun handleMessage(websocket: ServerWebSocket, message: JsonObject) {
        try {
            val connectionId = connectionCache.getConnectionIdForCommandSocket(websocket)
            if (connectionId != null) {
                // Update last activity timestamp
                connectionStore.updateLastActivity(connectionId)

                // Check for heartbeat response
                if (message.getString("type") == "heartbeat_response") {
                    handleHeartbeatResponse(connectionId, message)
                } else {
                    // Process regular message
                    messageHandler.handle(connectionId, message)
                }
            } else {
                throw IllegalStateException("No connection found for this websocket")
            }
        } catch (e: Exception) {
            handleError(websocket, e)
        }
    }

    private suspend fun handleClose(websocket: ServerWebSocket) {
        try {
            val connectionId = connectionCache.getConnectionIdForCommandSocket(websocket)
            if (connectionId != null) {
                log.info("Command socket closed for connection {}", connectionId)

                // Stop the heartbeat
                stopHeartbeat(connectionId)

                // Instead of immediate cleanup, keep connection for potential reconnection
                // Set it as reconnectable for the reconnection window
                connectionStore.updateReconnectable(connectionId, true)

                // Remove socket from cache but don't delete connection yet
                connectionCache.removeCommandSocket(connectionId)

                // If reconnection doesn't happen, cleanup verticle will handle it later
                log.info("Connection {} marked for potential reconnection", connectionId)
            } else {
                log.warn("Command socket closed, but no connection ID found")
            }
        } catch (e: Exception) {
            log.error("Error handling websocket close", e)
        }
    }

    private fun handleError(websocket: ServerWebSocket, error: Throwable) {
        log.error("Error in command WebSocket connection", error)
        try {
            val errorMessage = JsonObject()
                .put("type", "error")
                .put("message", error.message)
                .put("timestamp", System.currentTimeMillis())

            websocket.writeTextMessage(errorMessage.encode())
        } catch (e: Exception) {
            log.error("Failed to send error message to client", e)
        } finally {
            // Don't close the socket here - let the client handle reconnection
            // Only close if it's a critical error during handshake
            val connectionId = connectionCache.getConnectionIdForCommandSocket(websocket)
            if (connectionId == null && !websocket.isClosed()) {
                websocket.close()
            }
        }
    }

    /**
     * Handle a heartbeat response from client
     */
    private fun handleHeartbeatResponse(connectionId: String, message: JsonObject) {
        try {
            // Calculate round-trip time
            val serverTimestamp = message.getLong("timestamp")
            val clientTimestamp = message.getLong("clientTimestamp", 0L)
            val now = System.currentTimeMillis()
            val roundTripTime = now - serverTimestamp

            // Only log occasionally to reduce noise
            if (Math.random() < 0.1) { // Log roughly 10% of heartbeat responses
                log.debug("Received heartbeat response from {}: round-trip={}ms", connectionId, roundTripTime)
            }
        } catch (e: Exception) {
            log.warn("Error processing heartbeat response: {}", e.message)
        }
    }

    /**
     * Start periodic heartbeat for a connection
     */
    private fun startHeartbeat(connectionId: String) {
        // Stop any existing heartbeat for this connection
        stopHeartbeat(connectionId)

        // Run heartbeat every 15 seconds
        val timerId = vertx.setPeriodic(HEARTBEAT_INTERVAL_MS) { _ ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val commandSocket = connectionCache.getCommandSocket(connectionId)
                    if (commandSocket == null || commandSocket.isClosed()) {
                        // Socket is gone, stop heartbeat
                        stopHeartbeat(connectionId)
                        return@launch
                    }

                    // Send heartbeat
                    val heartbeatMessage = JsonObject()
                        .put("type", "heartbeat")
                        .put("timestamp", System.currentTimeMillis())

                    commandSocket.writeTextMessage(heartbeatMessage.encode())

                    // Update last activity
                    connectionStore.updateLastActivity(connectionId)

                    // Only log occasionally to reduce noise
                    if (Math.random() < 0.05) { // Log roughly 5% of heartbeats
                        log.debug("Sent heartbeat to connection {}", connectionId)
                    }
                } catch (e: Exception) {
                    log.warn("Error sending heartbeat to connection {}: {}", connectionId, e.message)
                    // Stop heartbeat if socket appears to be permanently gone
                    if (e.message?.contains("Connection was closed") == true) {
                        stopHeartbeat(connectionId)
                    }
                }
            }
        }

        // Store timer ID for cancellation
        heartbeatTimers[connectionId] = timerId
        log.info("Started heartbeat for connection {}", connectionId)
    }

    /**
     * Stop heartbeat for a connection
     */
    private fun stopHeartbeat(connectionId: String) {
        heartbeatTimers.remove(connectionId)?.let { timerId ->
            vertx.cancelTimer(timerId)
            log.info("Stopped heartbeat for connection {}", connectionId)
        }
    }

    /**
     * Coordinated connection cleanup process
     */
    private suspend fun cleanupConnection(connectionId: String): Boolean {
        try {
            log.info("Starting coordinated cleanup for connection: {}", connectionId)

            // Step 1: Clean up channels
            val channelCleanupResult = cleanupConnectionChannels(connectionId)
            if (!channelCleanupResult) {
                log.warn("Channel cleanup failed for connection {}", connectionId)
            }

            // Step 2: Clean up sockets and stop heartbeat
            stopHeartbeat(connectionId)
            val updateSocket = connectionCache.getUpdateSocket(connectionId)
            val socketCleanupResult = cleanupConnectionSockets(connectionId, updateSocket != null)
            if (!socketCleanupResult) {
                log.warn("Socket cleanup failed for connection {}", connectionId)
            }

            // Step 3: Mark connection as not reconnectable
            try {
                connectionStore.updateReconnectable(connectionId, false)
            } catch (e: Exception) {
                log.warn("Error marking connection as not reconnectable: {}", connectionId, e)
            }

            // Step 4: Remove the connection from DB and cache
            try {
                // Try to delete from the database first
                connectionStore.delete(connectionId)
            } catch (e: Exception) {
                log.warn("Error deleting connection {} from database", connectionId, e)
                // Continue anyway - the connection might already be gone
            }

            // Final cleanup from cache
            try {
                connectionCache.removeConnection(connectionId)
            } catch (e: Exception) {
                log.warn("Error removing connection {} from cache", connectionId, e)
            }

            log.info("Connection {} fully cleaned up", connectionId)
            return true
        } catch (e: Exception) {
            log.error("Error during connection cleanup", e)

            // Do emergency cleanup as a last resort
            try {
                connectionCache.removeCommandSocket(connectionId)
                connectionCache.removeUpdateSocket(connectionId)
                connectionCache.removeConnection(connectionId)
                log.info("Emergency cleanup completed for connection {}", connectionId)
            } catch (innerEx: Exception) {
                log.error("Emergency cleanup also failed", innerEx)
            }

            return false
        }
    }

    /**
     * Cleans up channel memberships for a connection
     */
    private suspend fun cleanupConnectionChannels(connectionId: String): Boolean {
        try {
            // Get the channels this connection is in
            val channels = channelStore.getChannelsForClient(connectionId)

            if (channels.isEmpty()) {
                log.debug("No channels found for connection {}", connectionId)
                return true
            }

            // Remove the connection from each channel
            var success = true
            for (channelId in channels) {
                try {
                    val result = channelStore.removeClientFromChannel(channelId, connectionId)
                    if (!result) {
                        log.warn("Failed to remove connection {} from channel {}", connectionId, channelId)
                        success = false
                    }
                } catch (e: Exception) {
                    log.warn("Error removing connection {} from channel {}", connectionId, channelId, e)
                    success = false
                }
            }

            return success
        } catch (e: Exception) {
            log.error("Error cleaning up channels for connection {}", connectionId, e)
            return false
        }
    }

    /**
     * Cleans up sockets for a connection
     */
    private suspend fun cleanupConnectionSockets(connectionId: String, hasUpdateSocket: Boolean): Boolean {
        try {
            var success = true

            // Clean up command socket
            try {
                connectionCache.removeCommandSocket(connectionId)?.let { socket ->
                    if (!socket.isClosed()) {
                        try {
                            socket.close()
                        } catch (e: Exception) {
                            log.warn("Error closing command socket for {}", connectionId, e)
                        }
                    }
                }
            } catch (e: Exception) {
                log.warn("Error removing command socket for {}", connectionId, e)
                success = false
            }

            // Clean up update socket if it exists
            if (hasUpdateSocket) {
                try {
                    connectionCache.removeUpdateSocket(connectionId)?.let { socket ->
                        if (!socket.isClosed()) {
                            try {
                                socket.close()
                            } catch (e: Exception) {
                                log.warn("Error closing update socket for {}", connectionId, e)
                            }
                        }
                    }
                } catch (e: Exception) {
                    log.warn("Error removing update socket for {}", connectionId, e)
                    success = false
                }
            }

            return success
        } catch (e: Exception) {
            log.error("Error cleaning up sockets for connection {}", connectionId, e)
            return false
        }
    }

    /**
     * Handle entity-specific sync request
     */
    private suspend fun handleEntitySync(connectionId: String, entityId: String): Boolean {
        log.debug("Processing entity sync for entity {} from connection {}", entityId, connectionId)

        try {
            // Check if connection and command socket exist
            if (!connectionCache.hasConnection(connectionId)) {
                log.warn("Connection {} not found for entity sync", connectionId)
                return false
            }

            val commandSocket = connectionCache.getCommandSocket(connectionId)
            if (commandSocket == null || commandSocket.isClosed()) {
                log.warn("Command socket not available for connection {}", connectionId)
                return false
            }

            // Fetch the entity
            val entities = entityStore.findEntitiesByIds(listOf(entityId))

            if (entities.isEmpty()) {
                log.warn("Entity {} not found for sync", entityId)
                sendSyncErrorToClient(connectionId, "Entity not found")
                return false
            }

            val entity = entities[entityId]

            // Create a sync response message
            val syncData = JsonObject()
                .put(
                    "entities", JsonObject()
                        .put("_id", entity?._id)
                        .put("type", entity?.type)
                        .put("version", entity?.version)
                    // Add more entity fields as needed
                )
                .put("timestamp", System.currentTimeMillis())

            val syncMessage = JsonObject()
                .put("type", "entity_sync")
                .put("data", syncData)

            // Send the sync message to the client
            commandSocket.writeTextMessage(syncMessage.encode())
            log.debug("Entity sync data sent for entity {} to connection {}", entityId, connectionId)

            // Update last activity
            connectionStore.updateLastActivity(connectionId)

            return true
        } catch (e: Exception) {
            log.error("Error during entity sync for entity {}", entityId, e)
            sendSyncErrorToClient(connectionId, "Sync failed: ${e.message}")
            return false
        }
    }

    /**
     * Send a sync error message to the client
     */
    private fun sendSyncErrorToClient(connectionId: String, errorMessage: String) {
        val socket = connectionCache.getCommandSocket(connectionId)
        if (socket != null && !socket.isClosed()) {
            try {
                val errorResponse = JsonObject()
                    .put("type", "sync_error")
                    .put("error", errorMessage)
                    .put("timestamp", System.currentTimeMillis())

                socket.writeTextMessage(errorResponse.encode())
            } catch (e: Exception) {
                log.error("Error sending sync error to client {}", connectionId, e)
            }
        }
    }
}