package com.minare.core.operation.models

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.vertx.core.json.JsonObject

interface OperationSetMember {
    fun build(): JsonObject
}
