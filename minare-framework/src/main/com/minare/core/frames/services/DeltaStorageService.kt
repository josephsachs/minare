package com.minare.core.frames.services

import com.google.inject.Inject
import com.minare.core.frames.models.FrameDelta
import com.minare.core.operation.models.OperationType
import com.minare.core.storage.interfaces.DeltaStore
import com.minare.core.storage.interfaces.StateStore
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory

class DeltaStorageService @Inject constructor(
    private val deltaStore: DeltaStore,
    private val stateStore: StateStore
) {
    private val log = LoggerFactory.getLogger(DeltaStorageService::class.java)

    /**
     *
     */
    suspend fun captureAndStoreDelta(
        frameNumber: Long,
        entityId: String,
        operationType: OperationType,
        operationId: String,
        beforeState: JsonObject?,
        afterState: JsonObject?
    ) {
        // Create the delta record
        val delta = FrameDelta(
            frameNumber = frameNumber,
            entityId = entityId,
            operation = operationType,
            before = beforeState?.getJsonObject("state"),  // Extract just the state portion
            after = afterState?.getJsonObject("state"),    // Extract just the state portion
            version = afterState?.getLong("version") ?: 0L,
            timestamp = System.currentTimeMillis(),
            operationId = operationId
        )

        // Store it
        storeDelta(delta)

        log.debug("Captured delta for entity {} in frame {}: {} -> version {}",
            entityId, frameNumber, operationType, delta.version)
    }

    /**
     *
     */
    private suspend fun storeDelta(delta: FrameDelta) {
        try {
            deltaStore.appendDelta(delta.frameNumber, delta)

            log.trace("Stored delta for entity {} in frame {} (version {})",
                delta.entityId, delta.frameNumber, delta.version)

        } catch (e: Exception) {
            log.error("Failed to store delta for entity {} in frame {}",
                delta.entityId, delta.frameNumber, e)
            throw e
        }
    }
}