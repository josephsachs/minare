package com.minare.core.utils.vertx

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.frames.services.WorkerRegistry
import io.vertx.core.Promise
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory

/**
 * Uses a promise to await an event
 */
@Singleton
class EventWaiter @Inject constructor(
    private val workerRegistry: WorkerRegistry,
    private val vertx: Vertx
) {
    private val log = LoggerFactory.getLogger(EventWaiter::class.java)

    suspend fun waitFor(
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

    suspend fun waitForAll(
        eventAddress: String,
        condition: (JsonObject) -> Boolean = { true }
    ): Map<String, JsonObject> {
        val promise = Promise.promise<Map<String, JsonObject>>()
        val responses = mutableMapOf<String, JsonObject>()

        val consumer = vertx.eventBus().consumer<JsonObject>(eventAddress) { msg ->
            if (condition(msg.body())) {
                val workerId = msg.body().getString("workerId")

                responses[workerId] = msg.body()

                if (responses.keys == workerRegistry.getActiveWorkers()) {
                    promise.complete(responses.toMap())
                }
            }
        }

        try {
            return promise.future().await()
        } finally {
            consumer.unregister()
        }
    }
}