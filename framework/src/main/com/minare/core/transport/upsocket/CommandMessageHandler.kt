package com.minare.worker.upsocket

import com.minare.cache.ConnectionCache
import com.minare.core.operation.MutationVerticle
import io.vertx.core.Vertx
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Handles command messages after they've been processed through Kafka.
 * This is now called by MessageQueueConsumerVerticle instead of directly
 * from the WebSocket handler, meaning we now expect JsonObject messages with the
 * format of Operation.
 */
@Singleton
open class CommandMessageHandler @Inject constructor(
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

        if (!connectionCache.hasConnection(connectionId)) {
            log.warn("Connection not found in cache: {}", connectionId)
            throw IllegalArgumentException("Invalid connection ID: $connectionId")
        }

        when (command) {
            "mutate" -> handleMutate(connectionId, message)
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
        val entityObject = message.getJsonObject("entity")
        val entityId = entityObject?.getString("_id")

        if (entityId == null) {
            log.error("Mutate command missing entity ID")
            sendErrorToClient(connectionId, "Missing entity ID")
            return
        }

        try {
            val mutationCommand = JsonObject()
                .put("connectionId", connectionId)
                .put("entity", entityObject)

            try {
                val response = vertx.eventBus().request<JsonObject>(
                    MutationVerticle.ADDRESS_MUTATION,
                    mutationCommand
                ).await().body()

                sendSuccessToClient(connectionId, response.getJsonObject("entity"))

            } catch (e: ReplyException) {
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

        val socket = connectionCache.getUpSocket(connectionId)
        if (socket != null && !socket.isClosed()) {
            socket.writeTextMessage(response.encode())
        } else {
            log.warn("Cannot send success response: up socket not found or closed for {}", connectionId)
        }
    }

    /**
     * Send an error response to the client
     */
    private suspend fun sendErrorToClient(connectionId: String, errorMessage: String) {
        val response = JsonObject()
            .put("type", "mutation_error")
            .put("error", errorMessage)

        val socket = connectionCache.getUpSocket(connectionId)
        if (socket != null && !socket.isClosed()) {
            socket.writeTextMessage(response.encode())
        } else {
            log.warn("Cannot send error response: up socket not found or closed for {}", connectionId)
        }
    }
}