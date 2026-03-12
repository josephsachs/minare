package com.minare.core.operation.models

import io.vertx.core.json.JsonObject

interface OperationSetMember {
    fun build(): JsonObject
}
