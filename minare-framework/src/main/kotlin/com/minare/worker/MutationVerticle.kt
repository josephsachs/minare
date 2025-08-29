package com.minare.worker

import com.minare.controller.EntityController
import com.minare.core.entity.ReflectionCache
import com.minare.entity.MutationService
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.dispatcher
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject
import com.minare.persistence.StateStore

/**
 * Dedicated verticle for processing mutation commands.
 * This isolates mutation processing from the connection handling.
 *
 * Updated to work directly with JsonObjects instead of Entity objects.
 */
class MutationVerticle @Inject constructor(
    private val stateStore: StateStore,
    private val mutationService: MutationService
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(MutationVerticle::class.java)

    companion object {
        const val ADDRESS_MUTATION = "minare.mutation.process"
    }

    override suspend fun start() {
        vertx.eventBus().consumer<JsonObject>(ADDRESS_MUTATION).handler { message ->
            launch(vertx.dispatcher()) {
                try {
                    val command = message.body()
                    val connectionId = command.getString("connectionId")
                    val entityObject = command.getJsonObject("entity")
                    val entityId = entityObject?.getString("_id")
                    val entityType = entityObject?.getString("type")

                    if (entityId == null) {
                        message.fail(400, "Missing entity ID")
                        return@launch
                    }

                    if (entityType == null) {
                        message.fail(400, "Missing entity type")
                        return@launch
                    }

                    log.debug("Processing mutation for entity '$entityId' from connection '$connectionId'")

                    try {
                        val entityJson = stateStore.findEntityJson(entityId)

                        if (entityJson == null) {
                            message.fail(404, "Entity not found: $entityId")
                            return@launch
                        }

                        val result = mutationService.mutate(entityId, entityType, entityObject)

                        if (result.getBoolean("success", false)) {
                            val updatedEntityJson = stateStore.findEntityJson(entityId)

                            val response = JsonObject()
                                .put("success", true)
                                .put("entity", JsonObject()
                                    .put("_id", entityId)
                                    .put("version", updatedEntityJson?.getLong("version"))
                                    .put("type", updatedEntityJson?.getString("type"))
                                )

                            message.reply(response)
                        } else {
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