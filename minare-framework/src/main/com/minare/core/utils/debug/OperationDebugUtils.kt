package com.minare.core.utils.debug

import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Inject

class OperationDebugUtils @Inject constructor() {
    private val log = LoggerFactory.getLogger(OperationDebugUtils::class.java)

    fun logOperation(operationArray: JsonArray, label: String) {
        try {
            // Process each operation in the array
            for (i in 0 until operationArray.size()) {
                val operationJson = operationArray.getJsonObject(i)
                val operationId = operationJson.getString("id", "unknown")
                val entityId = operationJson.getString("entityId", "unknown")  // Note: entityId, not entity
                val traceId = operationJson.getString("traceId", "unknown")

                log.info("OPERATION_FLOW: id: {}, entityId: {}, traceId: {}, stage: {}",
                    operationId, entityId, traceId, label)
            }

        } catch (e: Exception) {
            log.error("Error processing Kafka record: {}", e.message, e)
            log.error("Array value was: {}", operationArray)
        }
    }

    fun logOperation(operationJson: JsonObject, label: String) {
        try {
            val operationId = operationJson.getString("id", "unknown")
            val entityId = operationJson.getString("entityId", "unknown")  // Note: entityId, not entity
            val traceId = operationJson.getString("traceId", "unknown")

            log.info("OPERATION_FLOW: id: {}, entityId: {}, traceId: {}, stage: {}",
                operationId, entityId, traceId, label)

        } catch (e: Exception) {
            log.error("Error processing Kafka record: {}", e.message, e)
            log.error("Array value was: {}", operationJson)
        }
    }

    fun logOperation(operations: List<JsonObject>, label: String) {
        try {
            // Process each operation in the list
            for (operationJson in operations) {
                val operationId = operationJson.getString("id", "unknown")
                val entityId = operationJson.getString("entityId", "unknown")
                val traceId = operationJson.getString("traceId", "unknown")

                log.info("OPERATION_FLOW: id: {}, entityId: {}, traceId: {}, stage: {}",
                    operationId, entityId, traceId, label)
            }

        } catch (e: Exception) {
            log.error("Error processing operations: {}", e.message, e)
            log.error("List size was: {}", operations.size)
        }
    }

    fun logManifestCheck(manifestKey: String, manifestExists: Boolean) {
        log.info("OPERATION_FLOW: Manifest check - key: {}, exists: {}",
            manifestKey,
            manifestExists
        )
    }
}