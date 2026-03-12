package com.minare.core.operation.models

import io.vertx.core.json.JsonObject

interface OperationSetStrategy {
    fun build(): JsonObject
}