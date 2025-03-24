package com.minare.core.websocket

import com.minare.worker.CommandSocketVerticle
import io.vertx.core.Vertx
import io.vertx.core.http.ServerWebSocket
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import javax.inject.Inject

/**
 * Configures WebSocket routes for the application.
 * Sets up both command and update socket endpoints.
 */
class WebSocketRoutes @Inject constructor(
    private val updateSocketManager: UpdateSocketManager,
    private val vertx: Vertx
) {
    /**
     * Registers WebSocket routes on the provided router.
     */
    fun register(router: Router) {
        // Command socket route - now handled through the event bus to CommandSocketVerticle
        router.route("/ws").handler { routingContext ->
            routingContext.request().toWebSocket()
                .onSuccess { socket ->
                    // Store socket in application context with a UUID
                    val socketId = "ws-" + java.util.UUID.randomUUID().toString()
                    vertx.sharedData().getLocalMap<String, ServerWebSocket>("websockets").put(socketId, socket)

                    // Notify CommandSocketVerticle via event bus
                    vertx.eventBus().request<JsonObject>(
                        CommandSocketVerticle.ADDRESS_COMMAND_SOCKET_HANDLE,
                        JsonObject().put("socketId", socketId)
                    )
                        .onFailure { err ->
                            routingContext.response()
                                .setStatusCode(500)
                                .end("Failed to process WebSocket connection: ${err.message}")
                        }
                }
                .onFailure { err ->
                    routingContext.response()
                        .setStatusCode(400)
                        .end("Command WebSocket upgrade failed")
                }
        }

        // Update socket route - for server updates
        router.route("/ws/updates").handler { routingContext ->
            routingContext.request().toWebSocket()
                .onSuccess(updateSocketManager)
                .onFailure { err ->
                    routingContext.response()
                        .setStatusCode(400)
                        .end("Update WebSocket upgrade failed")
                }
        }
    }
}