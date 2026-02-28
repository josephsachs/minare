package com.minare.core.frames.worker

import com.minare.controller.EntityController
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.services.EntityPublishService
import com.minare.core.frames.services.DeltaStorageService
import com.minare.core.operation.models.OperationType
import com.minare.core.storage.interfaces.ContextStore
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import com.minare.controller.OperationController
import com.minare.core.entity.graph.EntityGraphReferenceService
import kotlin.system.measureTimeMillis

/**
 * Handles operation processing for frame workers.
 * Receives operations from FrameWorkerVerticle and processes them
 * according to their type (MUTATE, CREATE, DELETE).
 */
class WorkerOperationHandlerVerticle @Inject constructor(
    private val vertx: Vertx,
    private val verticleLogger: VerticleLogger,
    private val deltaStorageService: DeltaStorageService,
    private val stateStore: StateStore,
    private val entityFactory: EntityFactory,
    private val entityController: EntityController,
    private val operationController: OperationController,
    private val publishService: EntityPublishService,
    private val entityGraphReferenceService: EntityGraphReferenceService,
    private val contextStore: ContextStore
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(WorkerOperationHandlerVerticle::class.java)
    private val debugTraceLogs: Boolean = false

    override suspend fun start() {
        log.info("Starting WorkerOperationHandlerVerticle")
        setupOperationHandlers()
        log.info("WorkerOperationHandlerVerticle started - operation handlers registered")
    }

    /**
     * Set up event bus handlers for each operation type.
     * Each operation type has its own handler with specific requirements.
     */
    private fun setupOperationHandlers() {
        vertx.eventBus().consumer<JsonObject>("worker.process.MUTATE") { message ->
            launch {
                try {
                    processMutateOperation(message.body())
                    message.reply(JsonObject().put("success", true))
                } catch (e: Exception) {
                    log.error("Failed to process MUTATE operation", e)
                    message.fail(500, e.message)
                }
            }
        }

        vertx.eventBus().consumer<JsonObject>("worker.process.CREATE") { message ->
            launch {
                try {
                    processCreateOperation(message.body())
                    message.reply(JsonObject().put("success", true))
                } catch (e: Exception) {
                    log.error("Failed to process CREATE operation", e)
                    message.fail(500, e.message)
                }
            }
        }

        vertx.eventBus().consumer<JsonObject>("worker.process.DELETE") { message ->
            launch {
                try {
                    processDeleteOperation(message.body())
                    message.reply(JsonObject().put("success", true))
                } catch (e: Exception) {
                    log.error("Failed to process DELETE operation", e)
                    message.fail(500, e.message)
                }
            }
        }

        log.info("Registered operation handlers for MUTATE, CREATE, DELETE")
    }

    // ===== MUTATE =====

    /**
     * Process a MUTATE operation.
     * Requires an existing entity, captures before/after states for delta storage.
     */
    private suspend fun processMutateOperation(context: JsonObject) {
        val frameNumber = context.getLong("frameNumber")
            ?: throw IllegalArgumentException("Frame number required")
        val operationJson = context.getJsonObject("operation")
        val entityId = extractEntityId(operationJson)
            ?: throw IllegalArgumentException("Entity ID required for MUTATE operation")
        val entityType = extractEntityType(operationJson)
            ?: throw IllegalArgumentException("Entity type required for MUTATE operation")
        val operationId = extractOperationId(operationJson)

        if (debugTraceLogs) {
            log.info("Processing MUTATE operation {} for entity {}", operationId, entityId)
        }

        // Capture BEFORE state
        val beforeEntity = stateStore.findOneJson(entityId)
            ?: throw IllegalStateException("Entity $entityId not found before mutation")

        val mutationCommand = buildMutationCommand(operationJson, entityId, entityType)

        if (debugTraceLogs) verticleLogger.logInfo("Processing mutate command for entity $entityId")

        val processingTime = measureTimeMillis {
            val result = executeMutation(mutationCommand)

            if (!result.getBoolean("success", false)) {
                throw Exception(result.getString("error", "Mutation failed"))
            }

            // Capture the AFTER state
            val afterEntity = stateStore.findOneJson(entityId)
                ?: throw IllegalStateException("Entity $entityId not found after mutation")

            deltaStorageService.captureAndStoreDelta(
                frameNumber = frameNumber,
                entityId = entityId,
                operationType = OperationType.MUTATE,
                operationId = operationId,
                operationJson = operationJson,
                beforeEntity = beforeEntity,
                afterEntity = afterEntity
            )
        }

        if (debugTraceLogs) log.info("Processed MUTATE operation {} for entity {} in {}ms", operationId, entityId, processingTime)
    }

    /**
     * Process a CREATE operation.
     * Creates a new entity, no before state exists.
     */
    private suspend fun processCreateOperation(context: JsonObject) {
        val frameNumber = context.getLong("frameNumber")
            ?: throw IllegalArgumentException("Frame number required")
        val operationJson = context.getJsonObject("operation")
        val entityType = extractEntityType(operationJson)
            ?: throw IllegalArgumentException("Entity type required for CREATE operation")
        val operationId = extractOperationId(operationJson)
        val delta = operationJson.getJsonObject("delta") ?: JsonObject()

        log.debug("Processing CREATE operation {} for type {}", operationId, entityType)

        val processingTime = measureTimeMillis {
            val entity = entityFactory.getNew(entityType)
            entity.type = entityType

            stateStore.setEntityState(entity, entityType, delta)
            stateStore.setEntityProperties(entity, entityType, delta)

            val savedEntity = entityController.create(entity)

            val afterEntity = stateStore.findOneJson(savedEntity._id)
                ?: throw IllegalStateException("Entity ${savedEntity._id} not found after creation")

            deltaStorageService.captureAndStoreDelta(
                frameNumber = frameNumber,
                entityId = savedEntity._id,
                operationType = OperationType.CREATE,
                operationId = operationId,
                operationJson = operationJson,
                beforeEntity = null,
                afterEntity = afterEntity
            )

            operationController.afterCreateOperation(operationJson, entity)

            val afterState = afterEntity.getJsonObject("state") ?: JsonObject()
            publishService.publishStateChange(
                savedEntity._id,
                entityType,
                savedEntity.version,
                afterState
            )

            log.debug("Created entity {} of type {} with version {}", savedEntity._id, entityType, savedEntity.version)
        }

        log.debug("Processed CREATE operation {} in {}ms", operationId, processingTime)
    }

    /**
     * Process a DELETE operation.
     * Removes an existing entity, captures final state before deletion.
     */
    private suspend fun processDeleteOperation(context: JsonObject) {
        val frameNumber = context.getLong("frameNumber")
            ?: throw IllegalArgumentException("Frame number required")
        val operationJson = context.getJsonObject("operation")
        val entityId = extractEntityId(operationJson)
            ?: throw IllegalArgumentException("Entity ID required for DELETE operation")
        val entityType = extractEntityType(operationJson)
            ?: throw IllegalArgumentException("Entity type required for DELETE operation")
        val operationId = extractOperationId(operationJson)

        log.debug("Processing DELETE operation {} for entity {}", operationId, entityId)

        val processingTime = measureTimeMillis {
            val beforeEntity = stateStore.findOneJson(entityId)
                ?: throw IllegalStateException("Entity $entityId not found for deletion")

            val beforeVersion = beforeEntity.getLong("version") ?: 0L

            // Publish delete notification BEFORE removing from channels,
            // so downstream handlers can still route via channel membership
            publishService.publishStateChange(
                entityId,
                entityType,
                beforeVersion,
                JsonObject().put("_deleted", true)
            )

            entityGraphReferenceService.removeReferencesToEntity(entityId)
            entityController.delete(entityId)

            deltaStorageService.captureAndStoreDelta(
                frameNumber = frameNumber,
                entityId = entityId,
                operationType = OperationType.DELETE,
                operationId = operationId,
                operationJson = operationJson,
                beforeEntity = beforeEntity,
                afterEntity = null
            )

            // Build a lightweight entity for the application hook
            val entity = entityFactory.getNew(entityType).apply {
                _id = entityId
                type = entityType
            }

            // Application hook — channel removal is the application's responsibility,
            // mirroring how afterCreateOperation adds entities to channels
            operationController.afterDeleteOperation(operationJson, entity)

            log.debug("Deleted entity {} of type {}", entityId, entityType)
        }

        log.debug("Processed DELETE operation {} in {}ms", operationId, processingTime)
    }

    /**
     * Extract operation ID.
     * Required for tracking and delta storage.
     */
    private fun extractOperationId(operationJson: JsonObject): String =
        operationJson.getString("id")
            ?: throw IllegalArgumentException("Operation ID required")

    /**
     * Extract entity ID from operation.
     * Returns null if not present (valid for some CREATE operations).
     */
    private fun extractEntityId(operationJson: JsonObject): String? =
        operationJson.getString("entityId")

    /**
     * Extract entity type from operation.
     * Returns null if not present.
     */
    private fun extractEntityType(operationJson: JsonObject): String? =
        operationJson.getString("entityType")


    /**
     * Build the mutation command structure expected by MutationVerticle.
     */
    private fun buildMutationCommand(
        operationJson: JsonObject,
        entityId: String,
        entityType: String
    ): JsonObject {
        return JsonObject()
            .put("command", "mutate")
            .put("entity", JsonObject()
                .put("_id", entityId)
                .put("type", entityType)
                .put("version", operationJson.getLong("version"))
                .put("state", operationJson.getJsonObject("delta") ?: JsonObject())
            )
    }

    /**
     * Execute mutation via event bus to MutationVerticle.
     */
    private suspend fun executeMutation(mutationCommand: JsonObject): JsonObject {
        return vertx.eventBus()
            .request<JsonObject>("minare.mutation.process",
                JsonObject().put("entity", mutationCommand.getJsonObject("entity"))
            )
            .await()
            .body()
    }

    override suspend fun stop() {
        log.info("Stopping WorkerOperationHandlerVerticle")
        super.stop()
    }
}