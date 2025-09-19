package com.minare.core.frames.worker

import com.minare.core.frames.services.DeltaStorageService
import com.minare.core.operation.models.OperationType
import com.minare.core.storage.interfaces.StateStore
import com.minare.core.utils.vertx.VerticleLogger
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.launch
import org.slf4j.LoggerFactory
import javax.inject.Inject
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
    private val stateStore: StateStore
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

    /**
     * Process a MUTATE operation.
     * Requires an existing entity, captures before/after states for delta storage.
     */
    private suspend fun processMutateOperation(operationJson: JsonObject) {
        val entityId = extractEntityId(operationJson)
            ?: throw IllegalArgumentException("Entity ID required for MUTATE operation")
        val entityType = operationJson.getString("entityType")
            ?: throw IllegalArgumentException("Entity type required for MUTATE operation")
        val frameNumber = extractFrameNumber(operationJson)
        val operationId = extractOperationId(operationJson)
        val connectionId = extractConnectionId(operationJson)

        log.debug("Processing MUTATE operation {} for entity {}", operationId, entityId)

        // Capture BEFORE state
        val beforeEntity = stateStore.findEntityJson(entityId)
            ?: throw IllegalStateException("Entity $entityId not found before mutation")

        val mutationCommand = buildMutationCommand(operationJson, entityId, entityType)

        if (debugTraceLogs) verticleLogger.logInfo("Processing mutate command for entity $entityId")

        val processingTime = measureTimeMillis {
            val result = executeMutation(mutationCommand, connectionId)

            if (!result.getBoolean("success", false)) {
                throw Exception(result.getString("error", "Mutation failed"))
            }

            // Capture the AFTER state
            val afterEntity = stateStore.findEntityJson(entityId)
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

        if (debugTraceLogs) log.info("Processed MUTATE for entity {} in {}ms", entityId, processingTime)
    }

    /**
     * Process a CREATE operation.
     * Creates a new entity, no before state exists.
     */
    private suspend fun processCreateOperation(operationJson: JsonObject) {
        val frameNumber = extractFrameNumber(operationJson)
        val operationId = extractOperationId(operationJson)
        val connectionId = extractConnectionId(operationJson)

        log.debug("Processing CREATE operation {}", operationId)

        // CREATE logic to be implemented
        // No entity ID yet - will be generated
        // No before state for delta storage

        log.warn("CREATE operation not yet fully implemented: {}", operationJson)
    }

    /**
     * Process a DELETE operation.
     * Removes an existing entity, captures final state before deletion.
     */
    private suspend fun processDeleteOperation(operationJson: JsonObject) {
        val entityId = extractEntityId(operationJson)
            ?: throw IllegalArgumentException("Entity ID required for DELETE operation")
        val frameNumber = extractFrameNumber(operationJson)
        val operationId = extractOperationId(operationJson)

        log.debug("Processing DELETE operation {} for entity {}", operationId, entityId)

        // DELETE logic to be implemented
        // Should capture before state for delta
        // After state would be null/deleted

        log.warn("DELETE operation not yet fully implemented: {}", operationJson)
    }

    // ===== Common extraction functions =====

    /**
     * Extract frame number from operation JSON.
     * Required for all operations for delta storage.
     */
    private fun extractFrameNumber(operationJson: JsonObject): Long =
        operationJson.getLong("frameNumber")
            ?: throw IllegalArgumentException("Frame number required")

    /**
     * Extract operation ID.
     * Required for tracking and delta storage.
     */
    private fun extractOperationId(operationJson: JsonObject): String =
        operationJson.getString("id")
            ?: throw IllegalArgumentException("Operation ID required")

    /**
     * Extract connection ID for tracking operation source.
     * Defaults to "frame-processor" if not present.
     */
    private fun extractConnectionId(operationJson: JsonObject): String =
        operationJson.getString("connectionId") ?: "frame-processor"

    /**
     * Extract entity ID from operation.
     * Returns null if not present (valid for CREATE operations).
     */
    private fun extractEntityId(operationJson: JsonObject): String? =
        operationJson.getString("entityId")

    // ===== MUTATE-specific helper functions =====

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
    private suspend fun executeMutation(
        mutationCommand: JsonObject,
        connectionId: String
    ): JsonObject {
        return vertx.eventBus()
            .request<JsonObject>("minare.mutation.process",
                JsonObject()
                    .put("connectionId", connectionId)
                    .put("entity", mutationCommand.getJsonObject("entity"))
            )
            .await()
            .body()
    }

    override suspend fun stop() {
        log.info("Stopping WorkerOperationHandlerVerticle")
        super.stop()
    }
}