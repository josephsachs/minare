package com.minare.core.utils.debug

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import java.util.UUID
import com.google.inject.Inject

class OperationDebugUtils @Inject constructor() {
    private val log = LoggerFactory.getLogger(OperationDebugUtils::class.java)

    fun logOperation(operationArray: JsonArray, label: String) {

            // Process each operation in the array
            for (i in 0 until operationArray.size()) {
                val operationJson = operationArray.getJsonObject(i)
                val operationId = operationJson.getString("id", "unknown")
                val entityId = operationJson.getString("entityId", "unknown")  // Note: entityId, not entity
                val traceId = operationJson.getString("traceId", "unknown")

                log.info("OPERATION_FLOW: $entityId, id: $operationId, traceId: $traceId, stage: $label")
            }

    }

    fun logOperation(operationJson: JsonObject, label: String) {
            val operationId = operationJson.getString("id", "unknown")
            val entityId = operationJson.getString("entityId", "unknown")  // Note: entityId, not entity
            val traceId = operationJson.getString("traceId", "unknown")

            log.info("OPERATION_FLOW: $entityId, id: $operationId, traceId: $traceId, stage: $label")
    }

    fun logOperation(operations: List<JsonObject>, label: String) {
            // Process each operation in the list
            for (operationJson in operations) {
                val operationId = operationJson.getString("id", "unknown")
                val entityId = operationJson.getString("entityId", "unknown")
                val traceId = operationJson.getString("traceId", "unknown")
                log.info("OPERATION_FLOW: $entityId, id: $operationId, traceId: $traceId, stage: $label")
            }
    }

    fun logManifestCheck(manifestKey: String, manifestExists: Boolean) {
        log.info("OPERATION_FLOW: Manifest check - key: {}, exists: {}",
            manifestKey,
            manifestExists
        )
    }
}