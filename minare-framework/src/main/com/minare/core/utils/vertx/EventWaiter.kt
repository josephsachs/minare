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
    // TEMPORARY DEBUG
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

        // TEMPORARY DEBUG
        log.info("TURN_CONTROLLER: WaitForEvent received waitForAll for $eventAddress")

        val consumer = vertx.eventBus().consumer<JsonObject>(eventAddress) { msg ->
            log.info("TURN_CONTROLLER: WaitForEvent received event $eventAddress with msg ${msg.body()}")

            if (condition(msg.body())) {
                log.info("TURN_CONTROLLER: WaitForEvent message satisfied condition $condition")
                val workerId = msg.body().getString("workerId")
                log.info("TURN_CONTROLLER: WaitForEvent message workerId $workerId")

                responses[workerId] = msg.body()

                log.info("TURN_CONTROLLER: WaitForEvent message responses now have ${responses.entries} " +
                        "and active workers ${workerRegistry.getActiveWorkers()}, " +
                        "condition is ${responses.keys == workerRegistry.getActiveWorkers()}")

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