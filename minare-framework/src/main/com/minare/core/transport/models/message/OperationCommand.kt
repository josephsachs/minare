package com.minare.core.transport.models.message

import com.minare.core.transport.models.Connection
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject

class OperationCommand constructor(
    var payload: JsonObject
) : CommandMessageObject {}