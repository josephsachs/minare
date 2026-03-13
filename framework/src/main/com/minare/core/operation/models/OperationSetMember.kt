package com.minare.core.operation.models

import com.fasterxml.jackson.annotation.JsonSubTypes
import com.fasterxml.jackson.annotation.JsonTypeInfo
import io.vertx.core.json.JsonObject

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "memberType")
@JsonSubTypes(
    JsonSubTypes.Type(value = Operation::class, name = "OPERATION"),
    JsonSubTypes.Type(value = FunctionCall::class, name = "FUNCTION_CALL"),
    JsonSubTypes.Type(value = Assert::class, name = "ASSERT"),
    JsonSubTypes.Type(value = Trigger::class, name = "TRIGGER")
)
interface OperationSetMember {
    fun build(): JsonObject
}
