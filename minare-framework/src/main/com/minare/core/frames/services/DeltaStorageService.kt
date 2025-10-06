package com.minare.core.frames.services

import com.google.inject.Inject
import com.minare.core.frames.models.FrameDelta
import com.minare.core.operation.models.OperationType
import com.minare.core.storage.interfaces.DeltaStore
import com.minare.core.storage.interfaces.StateStore
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

class DeltaStorageService @Inject constructor(
    private val deltaStore: DeltaStore
) {
    private val log = LoggerFactory.getLogger(DeltaStorageService::class.java)
    private val debugTraceLogs: Boolean = false

    /**
     *
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

        // Compute what actually changed based on the requested delta
        val actualChanges = if (beforeState != null && afterState != null && requestedDelta != null) {
            val changes = JsonObject()

            requestedDelta.fieldNames().forEach { field ->
                val oldValue = beforeState.getValue(field)
                val newValue = afterState.getValue(field)

                if (oldValue?.toString() != newValue?.toString()) {
                    changes.put(field, newValue)
                }
            }

            changes
        } else if (operationType == OperationType.CREATE && afterState != null) {
            // For CREATE, everything is new
            afterState
        } else {
            JsonObject()
        }

        if (actualChanges.isEmpty) {
            if (debugTraceLogs) log.trace("No actual changes for entity {} in frame {}", entityId, frameNumber)
            return
        }

        val prunedBefore = if (beforeState != null && requestedDelta != null) {
            val before = JsonObject()

            requestedDelta.fieldNames().forEach { field ->
                beforeState.getValue(field)?.let { before.put(field, it) }
            }

            before
        } else {
            null
        }

        val delta = FrameDelta(
            frameNumber = frameNumber,
            entityId = entityId,
            operation = operationType,
            before = prunedBefore,
            after = actualChanges,
            version = afterEntity?.getLong("version")
                ?: throw IllegalStateException("No version in afterEntity for $operationType operation"),
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
     *
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