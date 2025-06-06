package com.minare.worker

import com.minare.core.entity.ReflectionCache
import com.minare.core.models.Entity
import com.minare.persistence.EntityStore
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject
import com.minare.entity.EntityVersioningService
import com.minare.persistence.StateStore

/**
 * Dedicated verticle for processing mutation commands.
 * This isolates mutation processing from the connection handling.
 */
class MutationVerticle @Inject constructor(
    private val stateStore: StateStore,
    private val reflectionCache: ReflectionCache,
    private val versioningService: EntityVersioningService
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(MutationVerticle::class.java)

    companion object {
        const val ADDRESS_MUTATION = "minare.mutation.process"
    }

    override suspend fun start() {

        vertx.eventBus().consumer<JsonObject>(ADDRESS_MUTATION).handler { message ->
            launch(vertx.dispatcher()) {
                try {
                    // Extract command data
                    val command = message.body()
                    val connectionId = command.getString("connectionId")
                    val entityObject = command.getJsonObject("entity")
                    val entityId = entityObject?.getString("_id")

                    if (entityId == null) {
                        message.fail(400, "Missing entity ID")
                        return@launch
                    }

                    log.debug("Processing mutation for entity '$entityId' from connection '$connectionId'")

                    try {
                        // Find the entity to mutate
                        val entities: Map<String, Entity> = stateStore.findEntitiesByIds(listOf(entityId))

                        if (entities.isEmpty()) {
                            message.fail(404, "Entity not found: $entityId")
                            return@launch
                        }

                        // Get the entity and inject dependencies
                        val entity = entities[entityId]!!
                        entity.reflectionCache = this@MutationVerticle.reflectionCache
                        entity.stateStore = this@MutationVerticle.stateStore  // Changed from entityStore
                        entity.versioningService = this@MutationVerticle.versioningService  // New

                        // Perform the mutation
                        val result = entity.mutate(entityObject)

                        // If mutation was successful, fetch the updated entity
                        if (result.getBoolean("success", false)) {
                            val updatedEntity = stateStore.findEntitiesByIds(listOf(entityId))[entityId]

                            // Build response with the updated entity
                            val response = JsonObject()
                                .put("success", true)
                                .put("entity", JsonObject()
                                    .put("_id", updatedEntity?._id)
                                    .put("version", updatedEntity?.version)
                                    .put("type", updatedEntity?.type)
                                )

                            // Send successful response
                            message.reply(response)
                        } else {
                            // Return the error from the mutation
                            message.fail(400, result.getString("message", "Unknown error"))
                        }
                    } catch (e: Exception) {
                        log.error("Error during mutation processing for entity $entityId", e)
                        message.fail(500, "Internal error: ${e.message}")
                    }
                } catch (e: Exception) {
                    log.error("Failed to process mutation command", e)
                    message.fail(500, "Internal error: ${e.message}")
                }
            }
        }

        log.info("MutationVerticle started")
    }
}