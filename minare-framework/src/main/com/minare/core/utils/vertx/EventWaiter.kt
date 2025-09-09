package com.minare.core.utils.vertx

import com.google.inject.Inject
import com.google.inject.Singleton
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await

/**
 * Uses a promise to await an event
 */
@Singleton
class EventWaiter @Inject constructor(
    private val vertx: Vertx
) {
    suspend fun waitForEvent(
        eventAddress: String,
        condition: (JsonObject) -> Boolean = { true }
    ): JsonObject {
        val promise = Promise.promise<JsonObject>()

        val consumer = vertx.eventBus().consumer<JsonObject>(eventAddress) { msg ->
            if (condition(msg.body())) {
                promise.complete(msg.body())
            }
        }

        try {
            return promise.future().await()
        } finally {
            consumer.unregister()
        }
    }
}