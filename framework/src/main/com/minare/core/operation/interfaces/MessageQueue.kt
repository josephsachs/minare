package com.minare.core.operation.interfaces

import io.vertx.core.json.JsonArray


/**
 * Abstraction for the message broker handling Operations.
 */
interface MessageQueue {
    suspend fun send(topic: String, message: JsonArray)
    suspend fun send(topic: String, key: String, message: JsonArray)
}