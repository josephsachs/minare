package com.minare.core.websocket

import com.minare.controller.ConnectionController
import com.minare.core.entity.ReflectionCache
import com.minare.core.models.Entity
import com.minare.worker.MutationVerticle
import com.minare.persistence.EntityStore
import io.vertx.core.Vertx
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
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
    private val vertx: Vertx
) {
    private val log = LoggerFactory.getLogger(CommandMessageHandler::class.java)

    /**
     * Handles incoming command messages, dispatching to appropriate handlers
     * based on the command type
     */
    suspend fun handle(connectionId: String, message: JsonObject) {
        val command = message.getString("command")

        // Verify the connection exists in memory
        if (!connectionController.hasConnection(connectionId)) {
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
                log.error("Mutation failed: ${e.message}")
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

        val socket = connectionController.getCommandSocket(connectionId)
        if (socket != null && !socket.isClosed()) {
            socket.writeTextMessage(response.encode())
        }
    }

    /**
     * Send an error response to the client
     */
    private suspend fun sendErrorToClient(connectionId: String, errorMessage: String) {
        val response = JsonObject()
            .put("type", "mutation_error")
            .put("error", errorMessage)

        val socket = connectionController.getCommandSocket(connectionId)
        if (socket != null && !socket.isClosed()) {
            socket.writeTextMessage(response.encode())
        }
    }

    /**
     * Handle a sync command
     * Sync commands are used to request the current state of data
     */
    protected open suspend fun handleSync(connectionId: String, message: JsonObject) {
        log.debug("Handling sync command for connection {}: {}", connectionId, message)

        // Get connection from memory
        val connection = connectionController.getConnection(connectionId)

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

            val commandSocket = connectionController.getCommandSocket(connectionId)
            if (commandSocket != null && !commandSocket.isClosed()) {
                commandSocket.writeTextMessage(response.encode())
            }

            return
        }

        // Otherwise, handle entity-specific sync request
        val id = entityObject.getString("_id") ?: ""
        if (id.isEmpty()) {
            throw IllegalArgumentException("Sync command requires an entity with a valid _id")
        }

        // Stub implementation - to be overridden by application
        log.info("Sync operation on entity '{}' requested", id)
    }
}