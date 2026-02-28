package com.minare.worker.upsocket

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.operation.MutationVerticle
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.transport.upsocket.UpSocketVerticle
import io.vertx.core.Vertx
import io.vertx.core.eventbus.ReplyException
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory

@Singleton
open class CommandMessageHandler @Inject constructor(
    private val vertx: Vertx,
    private val connectionStore: ConnectionStore
) {
    private val log = LoggerFactory.getLogger(CommandMessageHandler::class.java)

    suspend fun handle(connectionId: String, message: JsonObject) {
        val command = message.getString("command")

        if (!connectionStore.exists(connectionId)) {
            throw IllegalArgumentException("Invalid connection ID: $connectionId")
        }

        when (command) {
            "mutate" -> handleMutate(connectionId, message)
            else -> throw IllegalArgumentException("Unknown command: $command")
        }
    }

    protected open suspend fun handleMutate(connectionId: String, message: JsonObject) {
        val entityObject = message.getJsonObject("entity")
        val entityId = entityObject?.getString("_id")

        if (entityId == null) {
            sendToClient(connectionId, JsonObject()
                .put("type", "mutation_error")
                .put("error", "Missing entity ID")
            )
            return
        }

        try {
            val response = vertx.eventBus().request<JsonObject>(
                MutationVerticle.ADDRESS_MUTATION,
                JsonObject()
                    .put("connectionId", connectionId)
                    .put("entity", entityObject)
            ).await().body()

            sendToClient(connectionId, JsonObject()
                .put("type", "mutation_success")
                .put("entity", response.getJsonObject("entity"))
            )
        } catch (e: ReplyException) {
            log.error("Mutation failed: {}", e.message)
            sendToClient(connectionId, JsonObject()
                .put("type", "mutation_error")
                .put("error", e.message ?: "Mutation failed")
            )
        } catch (e: Exception) {
            log.error("Error processing mutation command", e)
            sendToClient(connectionId, JsonObject()
                .put("type", "mutation_error")
                .put("error", "Internal error: ${e.message}")
            )
        }
    }

    private suspend fun sendToClient(connectionId: String, message: JsonObject) {
        try {
            val connection = connectionStore.find(connectionId)
            val deploymentId = connection.upSocketDeploymentId ?: return
            vertx.eventBus().send(
                "${UpSocketVerticle.ADDRESS_SEND_TO_CONNECTION}.${deploymentId}",
                JsonObject()
                    .put("connectionId", connectionId)
                    .put("message", message)
            )
        } catch (e: Exception) {
            log.warn("Failed to send message to connection {}: {}", connectionId, e.message)
        }
    }
}