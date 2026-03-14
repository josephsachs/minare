package com.minare.core.frames.worker

import com.minare.controller.ChannelController
import com.minare.controller.EntityController
import com.minare.core.entity.factories.EntityFactory
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
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Trigger
import com.minare.core.entity.services.EntityFieldDeserializer
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.kotlinFunction
import kotlin.system.measureTimeMillis

/**
 * Handles operation processing for frame workers.
 * Receives operations from FrameWorkerVerticle and processes them
 * according to their type (MUTATE, CREATE, DELETE).
 *
 * CREATE and DELETE broadcast updates directly to channels for immediate delivery.
 * MUTATE goes through the pub/sub path and is delivered on the tick.
 */
class WorkerOperationHandlerVerticle @Inject constructor(
    private val vertx: Vertx,
    private val verticleLogger: VerticleLogger,
    private val deltaStorageService: DeltaStorageService,
    private val stateStore: StateStore,
    private val entityFactory: EntityFactory,
    private val entityController: EntityController,
    private val operationController: OperationController,
    private val entityGraphReferenceService: EntityGraphReferenceService,
    private val contextStore: ContextStore,
    private val channelController: ChannelController,
    private val reflectionCache: ReflectionCache
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
                    val entityId = processCreateOperation(message.body())
                    message.reply(JsonObject().put("success", true).put("entityId", entityId))
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

        // Fire-and-forget handler for OperationSet Trigger steps.
        // Hydrates the target entity and invokes the @Trigger method asynchronously.
        vertx.eventBus().consumer<JsonObject>(OperationSetExecutor.ADDRESS_TRIGGER) { message ->
            launch {
                try {
                    processTrigger(message.body())
                } catch (e: Exception) {
                    log.error("Trigger execution failed", e)
                }
            }
        }

        log.info("Registered operation handlers for MUTATE, CREATE, DELETE, TRIGGER")
    }

    // ===== MUTATE =====

    /**
     * Process a MUTATE operation.
     * Requires an existing entity, captures before/after states for delta storage.
     * Mutations go through the pub/sub path and are delivered on the tick.
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

            operationController.afterMutateOperation(operationJson, beforeEntity, afterEntity)

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

    // ===== CREATE =====

    /**
     * Process a CREATE operation.
     * Creates a new entity, no before state exists.
     * Broadcasts the update directly to channels — no pub/sub, no tick delay.
     */
    private suspend fun processCreateOperation(context: JsonObject): String {
        val frameNumber = context.getLong("frameNumber")
            ?: throw IllegalArgumentException("Frame number required")
        val operationJson = context.getJsonObject("operation")
        val entityType = extractEntityType(operationJson)
            ?: throw IllegalArgumentException("Entity type required for CREATE operation")
        val operationId = extractOperationId(operationJson)
        val delta = operationJson.getJsonObject("delta") ?: JsonObject()

        log.debug("Processing CREATE operation {} for type {}", operationId, entityType)

        var createdEntityId = ""
        val processingTime = measureTimeMillis {
            val entity = entityFactory.getNew(entityType)
            entity.type = entityType

            stateStore.setEntityState(entity, entityType, delta)
            stateStore.setEntityProperties(entity, entityType, delta)

            val savedEntity = entityController.create(entity)
            createdEntityId = savedEntity._id

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

            val afterState = afterEntity.getJsonObject("state") ?: JsonObject()
            val entityUpdate = JsonObject()
                .put("type", entityType)
                .put("_id", savedEntity._id)
                .put("version", savedEntity.version)
                .put("operation", "update")
                .put("changedAt", System.currentTimeMillis())
                .put("delta", afterState)

            val updateMessage = JsonObject()
                .put("type", "update")
                .put("timestamp", System.currentTimeMillis())
                .put("updates", JsonObject().put(savedEntity._id, entityUpdate))

            // Application hook
            operationController.afterCreateOperation(operationJson, entity)

            val channels = contextStore.getChannelsByEntityId(savedEntity._id)
            for (channelId in channels) {
                channelController.broadcast(channelId, updateMessage)
            }

            log.debug("Created entity {} of type {} with version {}", savedEntity._id, entityType, savedEntity.version)
        }

        log.debug("Processed CREATE operation {} in {}ms", operationId, processingTime)
        return createdEntityId
    }

    // ===== DELETE =====

    /**
     * Process a DELETE operation.
     * Removes an existing entity, captures final state before deletion.
     * Broadcasts the delete notification directly to channels before cleanup —
     * no pub/sub, no race condition with channel removal.
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

            // Capture channels before any cleanup
            val channels = contextStore.getChannelsByEntityId(entityId)

            // Clean up channel membership, graph references, and delete entity
            channels.forEach { channelId ->
                contextStore.remove(entityId, channelId)
            }

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

            val entityUpdate = JsonObject()
                .put("type", entityType)
                .put("_id", entityId)
                .put("version", beforeVersion)
                .put("operation", "update")
                .put("changedAt", System.currentTimeMillis())
                .put("delta", JsonObject().put("_deleted", true))

            val updateMessage = JsonObject()
                .put("type", "update")
                .put("timestamp", System.currentTimeMillis())
                .put("updates", JsonObject().put(entityId, entityUpdate))

            // Application hook
            val entity = entityFactory.getNew(entityType).apply {
                _id = entityId
                type = entityType
            }
            operationController.afterDeleteOperation(operationJson, entity)

            for (channelId in channels) {
                channelController.broadcast(channelId, updateMessage)
            }

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

    // ===== TRIGGER =====

    /**
     * Process a fire-and-forget Trigger from an OperationSet.
     * Hydrates the target entity, finds the @Trigger method by name, and invokes it.
     * Runs asynchronously — the OperationSet executor does not wait for this.
     */
    private suspend fun processTrigger(payload: JsonObject) {
        val entityId = payload.getString("entityId")
            ?: throw IllegalArgumentException("entityId required for TRIGGER")
        val entityType = payload.getString("entityType")
            ?: throw IllegalArgumentException("entityType required for TRIGGER")
        val target = payload.getString("target")
            ?: throw IllegalArgumentException("target required for TRIGGER")

        val entityJson = stateStore.findOneJson(entityId)
            ?: throw IllegalStateException("Entity $entityId not found for TRIGGER")

        val entity = entityFactory.getNew(entityType).apply {
            _id = entityId
            version = entityJson.getLong("version")
            type = entityType
        }

        stateStore.setEntityState(entity, entityType, entityJson.getJsonObject("state", JsonObject()))
        stateStore.setEntityProperties(entity, entityType, entityJson.getJsonObject("properties", JsonObject()))

        val methods = reflectionCache.getFunctions(entity.javaClass)
        val method = methods.firstOrNull { it.name == target && it.isAnnotationPresent(Trigger::class.java) }
            ?: throw IllegalStateException("No @Trigger method '$target' on $entityType")

        method.isAccessible = true
        val kFunc = method.kotlinFunction

        if (kFunc != null) {
            // Pass pipe args if available
            val pipeJson = payload.getJsonObject("_pipe")
            val params = kFunc.parameters.drop(1)

            when {
                params.isEmpty() -> {
                    if (kFunc.isSuspend) kFunc.callSuspend(entity) else kFunc.call(entity)
                }
                params.size == 1 && pipeJson != null -> {
                    val arg = params[0].name?.let { pipeJson.getValue(it) } ?: pipeJson
                    if (kFunc.isSuspend) kFunc.callSuspend(entity, arg) else kFunc.call(entity, arg)
                }
                pipeJson is JsonObject -> {
                    val args = params.map { p -> p.name?.let { pipeJson.getValue(it) } }.toTypedArray()
                    if (kFunc.isSuspend) kFunc.callSuspend(entity, *args) else kFunc.call(entity, *args)
                }
                else -> {
                    if (kFunc.isSuspend) kFunc.callSuspend(entity) else kFunc.call(entity)
                }
            }
        }
    }

    override suspend fun stop() {
        log.info("Stopping WorkerOperationHandlerVerticle")
        super.stop()
    }
}