package com.minare.worker.upsocket

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.entity.services.MutationService
import com.minare.core.storage.interfaces.ConnectionStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.transport.upsocket.UpSocketVerticle
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

@Singleton
open class CommandMessageHandler @Inject constructor(
    private val vertx: Vertx,
    private val connectionStore: ConnectionStore,
    private val mutationService: MutationService,
    private val stateStore: StateStore
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
        val entityType = entityObject?.getString("type")

        if (entityId == null) {
            sendToClient(connectionId, JsonObject()
                .put("type", "mutation_error")
                .put("error", "Missing entity ID")
            )
            return
        }

        if (entityType == null) {
            sendToClient(connectionId, JsonObject()
                .put("type", "mutation_error")
                .put("error", "Missing entity type")
            )
            return
        }

        try {
            val beforeEntity = stateStore.findOneJson(entityId)
            if (beforeEntity == null) {
                sendToClient(connectionId, JsonObject()
                    .put("type", "mutation_error")
                    .put("error", "Entity not found: $entityId")
                )
                return
            }

            val result = mutationService.mutate(entityId, entityType, beforeEntity, entityObject)

            if (result is MutationService.MutateResult) {
                sendToClient(connectionId, JsonObject()
                    .put("type", "mutation_success")
                    .put("entity", JsonObject()
                        .put("_id", entityId)
                        .put("version", result.version)
                        .put("type", entityType)
                    )
                )
            } else {
                val rejection = result as JsonObject
                sendToClient(connectionId, JsonObject()
                    .put("type", "mutation_error")
                    .put("error", rejection.getString("message", "Mutation failed"))
                )
            }
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
            val deploymentId = connection.upSocketInstanceId ?: return
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