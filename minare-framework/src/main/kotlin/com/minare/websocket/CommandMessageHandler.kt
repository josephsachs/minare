package com.minare.core.websocket

import com.minare.cache.ConnectionCache
import com.minare.controller.ConnectionController
import com.minare.core.entity.ReflectionCache
import com.minare.worker.MutationVerticle
import com.minare.persistence.EntityStore
import com.minare.worker.command.CommandVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.core.Vertx
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton
import kotlin.coroutines.CoroutineContext

@Singleton
open class CommandMessageHandler @Inject constructor(
    private val connectionController: ConnectionController,
    private val coroutineContext: CoroutineContext,
    private val entityStore: EntityStore,
    private val reflectionCache: ReflectionCache,
    private val vertx: Vertx,
    private val connectionCache: ConnectionCache
) {
    private val log = LoggerFactory.getLogger(CommandMessageHandler::class.java)

    /**
     * Handles incoming command messages, dispatching to appropriate handlers
     * based on the command type
     */
    suspend fun handle(connectionId: String, message: JsonObject) {
        val command = message.getString("command")

        // Verify the connection exists in memory first for better performance
        if (!connectionCache.hasConnection(connectionId)) {
            log.warn("Connection not found in cache: {}", connectionId)
            throw IllegalArgumentException("Invalid connection ID: $connectionId")
        }

        when (command) {
            "mutate" -> handleMutate(connectionId, message)
            "sync" -> handleSync(connectionId, message)
            else -> {
                log.warn("Unknown command: {}", command)
                throw IllegalArgumentException("Unknown command: $command")
            }
        }
    }

    /**
     * Handle a mutate command by delegating to the MutationVerticle
     */
    protected open suspend fun handleMutate(connectionId: String, message: JsonObject) {
        log.debug("Handling mutate command for connection {}", connectionId)

        // Extract entity details for logging
        val entityObject = message.getJsonObject("entity")
        val entityId = entityObject?.getString("_id")

        if (entityId == null) {
            log.error("Mutate command missing entity ID")
            sendErrorToClient(connectionId, "Missing entity ID")
            return
        }

        log.debug("Delegating mutation for entity {} to MutationVerticle", entityId)

        try {
            // Create a message with the mutation command and connection ID
            val mutationCommand = JsonObject()
                .put("connectionId", connectionId)
                .put("entity", entityObject)

            // Send to MutationVerticle and await response
            try {
                val response = vertx.eventBus().request<JsonObject>(
                    MutationVerticle.ADDRESS_MUTATION,
                    mutationCommand
                ).await().body()

                // Send success response to client
                sendSuccessToClient(connectionId, response.getJsonObject("entity"))

            } catch (e: ReplyException) {
                // Handle error response from the verticle
                log.error("Mutation failed: {}", e.message)
                sendErrorToClient(connectionId, e.message ?: "Mutation failed")
            }

        } catch (e: Exception) {
            log.error("Error processing mutation command", e)
            sendErrorToClient(connectionId, "Internal error: ${e.message}")
        }
    }

    /**
     * Send a success response to the client
     */
    private suspend fun sendSuccessToClient(connectionId: String, entity: JsonObject?) {
        val response = JsonObject()
            .put("type", "mutation_success")
            .put("entity", entity)

        val socket = connectionCache.getCommandSocket(connectionId)
        if (socket != null && !socket.isClosed()) {
            socket.writeTextMessage(response.encode())
        } else {
            log.warn("Cannot send success response: command socket not found or closed for {}", connectionId)
        }
    }

    /**
     * Send an error response to the client
     */
    private suspend fun sendErrorToClient(connectionId: String, errorMessage: String) {
        val response = JsonObject()
            .put("type", "mutation_error")
            .put("error", errorMessage)

        val socket = connectionCache.getCommandSocket(connectionId)
        if (socket != null && !socket.isClosed()) {
            socket.writeTextMessage(response.encode())
        } else {
            log.warn("Cannot send error response: command socket not found or closed for {}", connectionId)
        }
    }

    /**
     * Handle a sync command
     * Sync commands are used to request the current state of data
     */
    protected open suspend fun handleSync(connectionId: String, message: JsonObject) {
        log.debug("Handling sync command for connection {}: {}", connectionId, message)

        // Check if this is a full channel sync request (no entity specified)
        val entityObject = message.getJsonObject("entity")

        if (entityObject == null) {
            // This is a full channel sync request
            log.info("Full channel sync requested for connection: {}", connectionId)

            // Use the existing connection controller method to sync all channels
            val success = connectionController.syncConnection(connectionId)

            // Send confirmation that sync was initiated
            val response = JsonObject()
                .put("type", "sync_initiated")
                .put("success", success)
                .put("timestamp", System.currentTimeMillis())

            val commandSocket = connectionCache.getCommandSocket(connectionId)
            if (commandSocket != null && !commandSocket.isClosed()) {
                commandSocket.writeTextMessage(response.encode())
            } else {
                log.warn("Cannot send sync initiated response: command socket not found or closed for {}", connectionId)
            }

            return
        }

        // Otherwise, handle entity-specific sync request
        val id = entityObject.getString("_id") ?: ""
        if (id.isEmpty()) {
            throw IllegalArgumentException("Sync command requires an entity with a valid _id")
        }

        // Handle entity-specific sync
        handleEntitySync(connectionId, id)
    }

    /**
     * Handle entity-specific sync request
     */
    private suspend fun handleEntitySync(connectionId: String, entityId: String) {
        log.debug("Entity sync requested for entity {} by connection {}", entityId, connectionId)

        try {
            // Delegate to the CommandSocketVerticle to handle the entity sync
            val syncCommand = JsonObject()
                .put("connectionId", connectionId)
                .put("entityId", entityId)

            // Send sync request to verticle
            vertx.eventBus().request<JsonObject>(
                CommandVerticle.ADDRESS_ENTITY_SYNC,
                syncCommand
            ).await()

            log.debug("Entity sync request for {} processed", entityId)
        } catch (e: Exception) {
            log.error("Error during entity sync for entity {}", entityId, e)
            sendErrorToClient(connectionId, "Failed to sync entity: ${e.message}")
        }
    }
}