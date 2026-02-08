package com.minare.core.frames.services

import com.google.inject.Inject
import com.minare.core.frames.models.FrameDelta
import com.minare.core.operation.models.OperationType
import com.minare.core.storage.interfaces.DeltaStore
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

class DeltaStorageService @Inject constructor(
    private val deltaStore: DeltaStore
) {
    private val log = LoggerFactory.getLogger(DeltaStorageService::class.java)
    private val debugTraceLogs: Boolean = false

    /**
     * Capture and store a delta for an operation.
     *
     * For MUTATE: beforeEntity and afterEntity both exist
     * For CREATE: beforeEntity is null, afterEntity exists
     * For DELETE: beforeEntity exists, afterEntity is null
     */
    suspend fun captureAndStoreDelta(
        frameNumber: Long,
        entityId: String,
        operationType: OperationType,
        operationId: String,
        operationJson: JsonObject,
        beforeEntity: JsonObject?,
        afterEntity: JsonObject?
    ) {
        val requestedDelta = operationJson.getJsonObject("delta")
        val beforeState = beforeEntity?.getJsonObject("state")
        val afterState = afterEntity?.getJsonObject("state")

        // Compute what actually changed based on operation type
        val actualChanges = when (operationType) {
            OperationType.MUTATE -> {
                // For MUTATE: compute what changed from before to after
                if (beforeState != null && afterState != null && requestedDelta != null) {
                    val changes = JsonObject()
                    requestedDelta.fieldNames().forEach { field ->
                        val oldValue = beforeState.getValue(field)
                        val newValue = afterState.getValue(field)
                        if (oldValue?.toString() != newValue?.toString()) {
                            changes.put(field, newValue)
                        }
                    }
                    changes
                } else {
                    JsonObject()
                }
            }
            OperationType.CREATE -> {
                // For CREATE: everything is new - record the full after state
                afterState ?: JsonObject()
            }
            OperationType.DELETE -> {
                // For DELETE: record what was removed - the full before state
                beforeState ?: JsonObject()
            }
        }

        if (actualChanges.isEmpty && operationType != OperationType.DELETE) {
            if (debugTraceLogs) log.trace("No actual changes for entity {} in frame {}", entityId, frameNumber)
            return
        }

        // For DELETE, we always want to store the delta even if state was empty
        // The delta itself represents the deletion event

        val prunedBefore = when (operationType) {
            OperationType.MUTATE -> {
                // For MUTATE: only include fields that were in the requested delta
                if (beforeState != null && requestedDelta != null) {
                    val before = JsonObject()
                    requestedDelta.fieldNames().forEach { field ->
                        beforeState.getValue(field)?.let { before.put(field, it) }
                    }
                    before
                } else {
                    null
                }
            }
            OperationType.CREATE -> {
                // For CREATE: no before state
                null
            }
            OperationType.DELETE -> {
                // For DELETE: full before state
                beforeState
            }
        }

        val prunedAfter = when (operationType) {
            OperationType.DELETE -> {
                // For DELETE: after state is empty (entity no longer exists)
                JsonObject()
            }
            else -> {
                actualChanges
            }
        }

        // Determine version based on operation type
        val version = when (operationType) {
            OperationType.DELETE -> {
                // For DELETE: use the before entity's version (entity is gone, no after version)
                beforeEntity?.getLong("version") ?: 0L
            }
            else -> {
                afterEntity?.getLong("version")
                    ?: throw IllegalStateException("No version in afterEntity for $operationType operation")
            }
        }

        val delta = FrameDelta(
            frameNumber = frameNumber,
            entityId = entityId,
            operation = operationType,
            before = prunedBefore,
            after = prunedAfter,
            version = version,
            timestamp = System.currentTimeMillis(),
            operationId = operationId
        )

        storeDelta(delta)

        if (debugTraceLogs) {
            log.debug("Captured delta for entity {} in frame {}: {} -> version {}",
                entityId, frameNumber, operationType, delta.version)
        }
    }

    /**
     * Store a delta to the delta store
     */
    private suspend fun storeDelta(delta: FrameDelta) {
        try {
            deltaStore.appendDelta(delta.frameNumber, delta)

            if (debugTraceLogs) {
                log.trace("Stored delta for entity {} in frame {} (version {})",
                    delta.entityId, delta.frameNumber, delta.version)
            }

        } catch (e: Exception) {
            log.error("Failed to store delta for entity {} in frame {}",
                delta.entityId, delta.frameNumber, e)
            throw e
        }
    }
}