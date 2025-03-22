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
    private val channelStore: ChannelStore,
    private val contextStore: ContextStore,
    private val messageHandler: CommandMessageHandler,
    private val reflectionCache: ReflectionCache,
    private val entityStore: EntityStore
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(CommandSocketVerticle::class.java)

    companion object {
        const val ADDRESS_COMMAND_SOCKET_HANDLE = "minare.command.socket.handle"
        const val ADDRESS_COMMAND_SOCKET_CLOSE = "minare.command.socket.close"
        const val ADDRESS_CONNECTION_CLEANUP = "minare.connection.cleanup"
        const val ADDRESS_CHANNEL_CLEANUP = "minare.channel.cleanup"
        const val ADDRESS_SOCKET_CLEANUP = "minare.socket.cleanup"
        const val ADDRESS_ENTITY_SYNC = "minare.entity.sync"
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
     */
    suspend fun handleCommandSocket(websocket: ServerWebSocket) {
        log.info("New command socket connection from {}", websocket.remoteAddress())

        websocket.textMessageHandler { message ->
            CoroutineScope(vertx.dispatcher()).launch {
                try {
                    val msg = JsonObject(message)
                    handleMessage(websocket, msg)
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

        // Create a new connection and register the socket
        CoroutineScope(vertx.dispatcher()).launch {
            initiateConnection(websocket)
        }
    }

    private suspend fun initiateConnection(websocket: ServerWebSocket) {
        try {
            val connection = connectionStore.create()

            // Store in cache - separate from the DB operation
            connectionCache.storeConnection(connection)

            // Register command socket - again, separate caching operation
            connectionCache.storeCommandSocket(connection._id, websocket, connection)

            // The database update is handled by the store
            connectionStore.updateSocketIds(
                connection._id,
                "cs-${java.util.UUID.randomUUID()}",
                null
            )

            sendConnectionConfirmation(websocket, connection._id)
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
                messageHandler.handle(connectionId, message)
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

                // Initiate coordinated cleanup via event bus
                val cleanupResult = vertx.eventBus().request<JsonObject>(
                    ADDRESS_CONNECTION_CLEANUP,
                    JsonObject().put("connectionId", connectionId)
                ).await()

                if (cleanupResult.body().getBoolean("success", false)) {
                    log.info("Connection {} fully cleaned up", connectionId)
                } else {
                    log.warn("Connection {} cleanup may be incomplete", connectionId)
                }
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

            websocket.writeTextMessage(errorMessage.encode())
        } finally {
            if (!websocket.isClosed()) {
                websocket.close()
            }
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

            // Step 2: Clean up sockets
            val updateSocket = connectionCache.getUpdateSocket(connectionId)
            val socketCleanupResult = cleanupConnectionSockets(connectionId, updateSocket != null)
            if (!socketCleanupResult) {
                log.warn("Socket cleanup failed for connection {}", connectionId)
            }

            // Step 3: Remove the connection from DB and cache
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
                .put("entities", JsonObject()
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