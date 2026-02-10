package com.minare.core.transport.upsocket.handlers

import com.minare.cache.ConnectionCache
import com.minare.core.storage.interfaces.ChannelStore
import com.minare.core.storage.interfaces.ContextStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.transport.models.message.SyncCommand
import com.minare.core.transport.models.message.SyncCommandType
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import com.google.inject.Singleton

/**
 * Handles sync commands outside of the Kafka flow.
 * TODO: Route sync thru downsocket instead of returning
 */
@Singleton
class SyncCommandHandler @Inject constructor(
    private val connectionCache: ConnectionCache,
    private val channelStore: ChannelStore,
    private val contextStore: ContextStore,
    private val stateStore: StateStore
) {
    private val log = LoggerFactory.getLogger(SyncCommandHandler::class.java)

    /**
     * Handle a sync command
     * Sync commands are used to request the current state of data
     */
    suspend fun handle(connectionId: String, message: JsonObject) {
        log.debug("Handling sync command for connection {}: {}", connectionId, message)

        // Check if this is a full channel sync request (no entity specified)
        val entityObject = message.getJsonObject("entity")

        if (entityObject == null) {
            log.info("Full channel sync requested for connection: {}", connectionId)

            val success = syncConnection(connectionId)

            val response = JsonObject()
                .put("type", "sync_initiated")
                .put("success", success)
                .put("timestamp", System.currentTimeMillis())

            val upSocket = connectionCache.getUpSocket(connectionId)
            if (upSocket != null && !upSocket.isClosed()) {
                upSocket.writeTextMessage(response.encode())
            } else {
                log.warn("Cannot send sync initiated response: up socket not found or closed for {}", connectionId)
            }

            return
        }

        val id = entityObject.getString("_id") ?: entityObject.getString("id")

        if (id == null) {
            log.error("Sync command missing entity ID")
            sendErrorToClient(connectionId, "Entity ID is required for entity sync")
            return
        }

        log.debug("Entity-specific sync not yet implemented for entity: {}", id)
        sendErrorToClient(connectionId, "Entity-specific sync not yet implemented")
    }

    /**
     * Send an error response to the client
     * @deprecated use MessageController
     */
    private suspend fun sendErrorToClient(connectionId: String, errorMessage: String) {
        val response = JsonObject()
            .put("type", "sync_error")
            .put("error", errorMessage)

        val socket = connectionCache.getUpSocket(connectionId)
        if (socket != null && !socket.isClosed()) {
            socket.writeTextMessage(response.encode())
        } else {
            log.warn("Cannot send error response: up socket not found or closed for {}", connectionId)
        }
    }

    /**
     * Receives messsage from MessageController and invokes connectionController appropriately
     */
    suspend fun tryHandle(message: SyncCommand) {
        if (message.type == SyncCommandType.CHANNEL) {
            if (message.select.isNullOrEmpty()) {
                syncConnection(message.connection._id)
            } else {
                throw NotImplementedError("Sync specific channel not implemented")
            }
        } else if (message.type == SyncCommandType.ENTITY) {
            throw NotImplementedError("Entity sync not implemented")
        } else {
            throw Exception("Unrecognized type")
        }
    }

    /**
     * Synchronize a connection with all entities in its channels
     * This is typically called when a connection is first established
     *
     * @param connectionId The ID of the connection to synchronize
     * @return True if sync was successful, false otherwise
     */
    suspend fun syncConnection(connectionId: String): Boolean {
        try {
            log.info("Starting sync for connection: {}", connectionId)

            val channels = channelStore.getChannelsForClient(connectionId)

            if (channels.isEmpty()) {
                log.info("No channels found for connection: {}", connectionId)
                return true
            }

            for (channelId in channels) {
                syncChannelToConnection(channelId, connectionId)
            }

            log.info("Sync completed for connection: {}", connectionId)
            return true

        } catch (e: Exception) {
            log.error("Error during connection sync: {}", connectionId, e)
            return false
        }
    }

    /**
     * Synchronize all entities in a channel to a specific connection
     *
     * @param channelId The channel containing the entities
     * @param connectionId The connection to sync to
     */
    suspend fun syncChannelToConnection(channelId: String, connectionId: String) {
        try {
            log.debug("Syncing channel {} to connection {}", channelId, connectionId)
            val entityIds = contextStore.getEntityIdsByChannel(channelId)

            if (entityIds.isEmpty()) {
                log.debug("No entities found in channel: {}", channelId)
                return
            }

            val entities = stateStore.findJsonByIds(entityIds).values  // Get just the values
            val syncData = JsonObject()
                .put("entities", JsonArray(entities.toList()))  // Convert to list for JsonArray
                .put("channelId", channelId)
                .put("timestamp", System.currentTimeMillis())

            val syncMessage = JsonObject()
                .put("type", "sync")
                .put("data", syncData)

            // TODO: Return sync results via downsocket instead
            val upSocket = connectionCache.getUpSocket(connectionId)

            // TODO: MIN-142 - Debug sync command attempts by null connections
            log.info("MINARE_SYNC_EVENT: Upsocket looks like this ${upSocket.toString()}")
            log.info("MINARE_SYNC_EVENT: Upsocket.isClosed: ${upSocket?.isClosed}")

            if (upSocket != null && !upSocket.isClosed) {
                upSocket.writeTextMessage(syncMessage.encode())
                log.debug("Sent sync data for channel {} to connection {}", channelId, connectionId)
            } else {
                log.warn("Cannot sync: up socket not found or closed for connection {}", connectionId)
            }

        } catch (e: Exception) {
            log.error("Error syncing channel {} to connection {}", channelId, connectionId, e)
        }
    }
}