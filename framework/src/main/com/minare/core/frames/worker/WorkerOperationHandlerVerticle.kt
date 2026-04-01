package com.minare.core.frames.worker

import com.minare.controller.ChannelController
import com.minare.controller.EntityController
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.frames.services.DeltaStorageService
import com.minare.core.operation.models.OperationResult
import com.minare.core.operation.models.OperationResult.Companion.ADDRESS_OPERATION_RESULT
import com.minare.core.operation.models.OperationType
import com.minare.core.storage.interfaces.ContextStore
import com.minare.core.storage.interfaces.StateStore
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import com.google.inject.Inject
import com.minare.controller.OperationController
import com.minare.controller.UpdateController
import com.minare.core.entity.graph.EntityGraphReferenceService
import com.minare.core.entity.ReflectionCache
import com.minare.core.entity.annotations.Trigger
import com.minare.core.entity.services.EntityPublishService
import com.minare.core.entity.services.MutationService
import kotlin.reflect.full.callSuspend
import kotlin.reflect.jvm.kotlinFunction
import java.util.concurrent.TimeUnit

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
    private val deltaStorageService: DeltaStorageService,
    private val stateStore: StateStore,
    private val entityFactory: EntityFactory,
    private val entityController: EntityController,
    private val updateController: UpdateController,
    private val operationController: OperationController,
    private val entityGraphReferenceService: EntityGraphReferenceService,
    private val contextStore: ContextStore,
    private val channelController: ChannelController,
    private val reflectionCache: ReflectionCache,
    private val mutationService: MutationService,
    private val publishService: EntityPublishService
) : CoroutineVerticle() {

    private val log = LoggerFactory.getLogger(WorkerOperationHandlerVerticle::class.java)

    private lateinit var instanceId: String

    override suspend fun start() {
        instanceId = config.getString("instanceId")
            ?: throw IllegalStateException("WorkerOperationHandlerVerticle requires instanceId in config")

        log.info("Starting WorkerOperationHandlerVerticle (instance: {})", instanceId)
        setupOperationHandlers()
        log.info("WorkerOperationHandlerVerticle started - operation handlers registered for instance {}", instanceId)
    }

    /**
     * Set up event bus handlers for each operation type.
     * Each handler is scoped to this instance's unique ID to prevent
     * Vert.x round-robin from breaking affinity guarantees.
     */
    private fun setupOperationHandlers() {
        vertx.eventBus().consumer<JsonObject>("worker.process.MUTATE.$instanceId") { message ->
            launch {
                val result = processMutateOperation(message.body())
                handleResult(result, message)
            }
        }

        vertx.eventBus().consumer<JsonObject>("worker.process.CREATE.$instanceId") { message ->
            launch {
                val result = processCreateOperation(message.body())
                handleResult(result, message)
            }
        }

        vertx.eventBus().consumer<JsonObject>("worker.process.DELETE.$instanceId") { message ->
            launch {
                val result = processDeleteOperation(message.body())
                handleResult(result, message)
            }
        }

        // Fire-and-forget handler for OperationSet Trigger steps.
        // Hydrates the target entity and invokes the @Trigger method asynchronously.
        vertx.eventBus().consumer<JsonObject>("${OperationSetExecutor.ADDRESS_TRIGGER}.$instanceId") { message ->
            launch {
                try {
                    processTrigger(message.body())
                } catch (e: Exception) {
                    log.error("Trigger execution failed", e)
                }
            }
        }

        log.info("Registered operation handlers for MUTATE, CREATE, DELETE, TRIGGER on instance {}", instanceId)
    }

    // ===== MUTATE =====

    private suspend fun processMutateOperation(context: JsonObject): OperationResult {
        val start = System.nanoTime()
        val operationJson = context.getJsonObject("operation")
        val frameNumber = context.getLong("frameNumber")
        val entityId = operationJson?.getString("entityId")
        val entityType = operationJson?.getString("entityType")
        val operationId = operationJson?.getString("id")

        try {
            val op = OperationType.MUTATE
            if (frameNumber == null) return rejected(start, op, entityId, operationId, null, "Frame number required")
            if (entityId == null) return rejected(start, op, null, operationId, frameNumber, "Entity ID required for MUTATE")
            if (entityType == null) return rejected(start, op, entityId, operationId, frameNumber, "Entity type required for MUTATE")
            if (operationId == null) return rejected(start, op, entityId, null, frameNumber, "Operation ID required")

            val mutationRequest = JsonObject()
                .put("_id", entityId)
                .put("type", entityType)
                .put("version", operationJson.getLong("version"))
                .put("state", operationJson.getJsonObject("delta") ?: JsonObject())

            // Version check + merge + snapshot in a single Lua EVAL — no separate read
            val result = mutationService.mutate(entityId, entityType, mutationRequest)

            if (result is JsonObject) {
                return rejected(start, op, entityId, operationId, frameNumber,
                    result.getString("message", "Mutation rejected"))
            }

            val mutateResult = result as MutationService.MutateResult

            // Publish state change (needs ContextStore, so stays in Kotlin)
            val delta = operationJson.getJsonObject("delta") ?: JsonObject()
            publishService.publishStateChange(entityId, entityType, mutateResult.version, delta)

            operationController.afterMutateOperation(operationJson, mutateResult.before, mutateResult.after)

            deltaStorageService.captureAndStoreDelta(
                frameNumber = frameNumber,
                entityId = entityId,
                operationType = OperationType.MUTATE,
                operationId = operationId,
                operationJson = operationJson,
                beforeEntity = mutateResult.before,
                afterEntity = mutateResult.after
            )

            return OperationResult.Success(entityId, op, operationId, frameNumber, elapsed(start))
        } catch (e: Exception) {
            return failed(start, OperationType.MUTATE, entityId, operationId, frameNumber, e)
        }
    }

    // ===== CREATE =====

    private suspend fun processCreateOperation(context: JsonObject): OperationResult {
        val start = System.nanoTime()
        val operationJson = context.getJsonObject("operation")
        val frameNumber = context.getLong("frameNumber")
        val entityType = operationJson?.getString("entityType")
        val operationId = operationJson?.getString("id")

        try {
            val op = OperationType.CREATE
            if (frameNumber == null) return rejected(start, op, null, operationId, null, "Frame number required")
            if (entityType == null) return rejected(start, op, null, operationId, frameNumber, "Entity type required for CREATE")
            if (operationId == null) return rejected(start, op, null, null, frameNumber, "Operation ID required")

            val delta = operationJson.getJsonObject("delta") ?: JsonObject()

            val entity = entityFactory.getNew(entityType)
            entity.type = entityType

            stateStore.setEntityState(entity, entityType, delta)
            stateStore.setEntityProperties(entity, entityType, delta)

            val savedEntity = entityController.create(entity)
            val entityId = savedEntity._id

            val afterEntity = stateStore.findOneJson(entityId)
                ?: return failed(start, op, entityId, operationId, frameNumber,
                    IllegalStateException("Entity $entityId not found after creation"))

            deltaStorageService.captureAndStoreDelta(
                frameNumber = frameNumber,
                entityId = entityId,
                operationType = OperationType.CREATE,
                operationId = operationId,
                operationJson = operationJson,
                beforeEntity = null,
                afterEntity = afterEntity
            )

            val afterState = afterEntity.getJsonObject("state") ?: JsonObject()
            val entityUpdate = JsonObject()
                .put("type", entityType)
                .put("_id", entityId)
                .put("version", savedEntity.version)
                .put("operation", "update")
                .put("changedAt", System.currentTimeMillis())
                .put("delta", afterState)

            val updateMessage = updateController.getUpdateMessage(entityId, entityUpdate)
            operationController.afterCreateOperation(operationJson, entity)

            val channels = contextStore.getChannelsByEntityId(entityId)
            for (channelId in channels) {
                channelController.broadcast(channelId, updateMessage)
            }

            return OperationResult.Success(entityId, op, operationId, frameNumber, elapsed(start))
        } catch (e: Exception) {
            return failed(start, OperationType.CREATE, null, operationId, frameNumber, e)
        }
    }

    // ===== DELETE =====

    private suspend fun processDeleteOperation(context: JsonObject): OperationResult {
        val start = System.nanoTime()
        val operationJson = context.getJsonObject("operation")
        val frameNumber = context.getLong("frameNumber")
        val entityId = operationJson?.getString("entityId")
        val entityType = operationJson?.getString("entityType")
        val operationId = operationJson?.getString("id")

        try {
            val op = OperationType.DELETE
            if (frameNumber == null) return rejected(start, op, entityId, operationId, null, "Frame number required")
            if (entityId == null) return rejected(start, op, null, operationId, frameNumber, "Entity ID required for DELETE")
            if (entityType == null) return rejected(start, op, entityId, operationId, frameNumber, "Entity type required for DELETE")
            if (operationId == null) return rejected(start, op, entityId, null, frameNumber, "Operation ID required")

            val beforeEntity = stateStore.findOneJson(entityId)
                ?: return rejected(start, op, entityId, operationId, frameNumber, "Entity $entityId not found for deletion")

            val beforeVersion = beforeEntity.getLong("version") ?: 0L

            // Capture channels before any cleanup
            val channels = contextStore.getChannelsByEntityId(entityId)

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

            val updateMessage = updateController.getUpdateMessage(entityId, entityUpdate)

            val entity = entityFactory.getNew(entityType).apply {
                _id = entityId
                type = entityType
            }
            operationController.afterDeleteOperation(operationJson, entity)

            for (channelId in channels) {
                channelController.broadcast(channelId, updateMessage)
            }

            return OperationResult.Success(entityId, op, operationId, frameNumber, elapsed(start))
        } catch (e: Exception) {
            return failed(start, OperationType.DELETE, entityId, operationId, frameNumber, e)
        }
    }

    // ===== Result handling =====

    private fun handleResult(result: OperationResult, message: io.vertx.core.eventbus.Message<JsonObject>) {
        val resultJson = result.toJson()
        vertx.eventBus().publish(ADDRESS_OPERATION_RESULT, resultJson)

        when (result) {
            is OperationResult.Success -> {
                message.reply(resultJson)
            }
            is OperationResult.Rejected -> {
                log.warn(OperationResult.formatFailure(result))
                message.fail(400, result.reason)
            }
            is OperationResult.Failed -> {
                log.error(OperationResult.formatFailure(result))
                message.fail(500, result.cause.message)
            }
        }
    }

    private fun elapsed(startNanos: Long): Long =
        TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - startNanos)

    private fun rejected(startNanos: Long, type: OperationType, entityId: String?, operationId: String?, frameNumber: Long?, reason: String) =
        OperationResult.Rejected(entityId, type, operationId, frameNumber, elapsed(startNanos), reason)

    private fun failed(startNanos: Long, type: OperationType, entityId: String?, operationId: String?, frameNumber: Long?, cause: Exception) =
        OperationResult.Failed(entityId, type, operationId, frameNumber, elapsed(startNanos), cause)

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