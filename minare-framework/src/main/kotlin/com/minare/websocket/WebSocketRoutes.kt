package com.minare.core.websocket

import io.vertx.ext.web.Router
import javax.inject.Inject

/**
 * Configures WebSocket routes for the application.
 * Sets up both command and update socket endpoints.
 */
class WebSocketRoutes @Inject constructor(
    private val commandSocketManager: CommandSocketManager,
    private val updateSocketManager: UpdateSocketManager
) {
    /**
     * Registers WebSocket routes on the provided router.
     */
    fun register(router: Router) {
        // Command socket route - for client requests
        router.route("/ws").handler { routingContext ->
            routingContext.request().toWebSocket()
                .onSuccess(commandSocketManager)
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