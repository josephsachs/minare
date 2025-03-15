package com.minare.core.websocket

import io.vertx.core.Future
import io.vertx.core.json.JsonObject
import org.slf4j.LoggerFactory
import javax.inject.Singleton

@Singleton
open class WebSocketMessageHandler {

    private val log = LoggerFactory.getLogger(WebSocketMessageHandler::class.java)

    open fun handle(connectionId: String, message: JsonObject): Future<Void> {
        val command = message.getString("command")

        return when (command) {
            "mutate" -> handleMutate(connectionId, message)
            "sync" -> handleSync(connectionId, message)
            else -> {
                log.warn("Unknown command: {}", command)
                Future.failedFuture("Unknown command: $command")
            }
        }
    }

    protected open fun handleMutate(connectionId: String, message: JsonObject): Future<Void> {
        log.debug("Handling mutate command for connection {}: {}", connectionId, message)
        // Stub implementation - to be overridden by concrete implementations
        return Future.succeededFuture()
    }

    protected open fun handleSync(connectionId: String, message: JsonObject): Future<Void> {
        log.debug("Handling sync command for connection {}: {}", connectionId, message)
        // Stub implementation - to be overridden by concrete implementations
        return Future.succeededFuture()
    }
}