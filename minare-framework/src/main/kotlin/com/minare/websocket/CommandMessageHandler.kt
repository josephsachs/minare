package com.minare.core.websocket

import com.minare.controller.ConnectionController
import com.minare.core.entity.ReflectionCache
import com.minare.core.models.Entity
import com.minare.persistence.EntityStore
import io.vertx.core.json.JsonObject
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
    private val reflectionCache: ReflectionCache
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
     * Handle a mutate command
     * Mutate commands are used to modify data in the database
     */
    protected open suspend fun handleMutate(connectionId: String, message: JsonObject) {
        log.debug("Handling mutate command for connection {}: {}", connectionId, message)

        // Get connection from memory
        val connection = connectionController.getConnection(connectionId)

        // Authentication would go here
        // if (!authenticator.validate(connection)) {
        //     throw SecurityException("Unauthorized")
        // }

        // Extract operation details
        // Assuming this is already within a suspending function
        val entityObject = message.getJsonObject("entity")
        val entityId = entityObject?.getString("_id")
        val version = entityObject?.getLong("version")
        val state = entityObject?.getJsonObject("state")

// Validate required fields
        if (entityId == null || version == null || state == null) {
            throw IllegalArgumentException("Mutate command requires id, version and new state")
        }

        log.info("Mutate operation on entity '{}' requested", entityId)

        try {
            // Using a functional approach
            val entities: Map<String, Entity> = entityStore.findEntitiesByIds(listOf(entityId))

            entities.forEach { (_, entity) ->
                // Manually inject just the reflection cache - this is what's causing the NPE
                entity.reflectionCache = this.reflectionCache
                entity.entityStore = this.entityStore

                // Call mutate with the entity object
                entity.mutate(entityObject)
            }
        } catch (e: Exception) {
            log.error("Tried to find nonexistent entity ${entityId}", e)
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