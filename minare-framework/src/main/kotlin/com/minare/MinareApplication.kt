package com.minare;

import com.minare.core.websocket.CommandSocketManager
import com.minare.core.websocket.UpdateSocketManager
import com.minare.persistence.MongoConnectionStore
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Promise
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler
import org.slf4j.LoggerFactory
import javax.inject.Inject

abstract class MinareApplication : AbstractVerticle() {
    val log = LoggerFactory.getLogger(MongoConnectionStore::class.java)

    @Inject lateinit var commandSocketManager: CommandSocketManager
    @Inject lateinit var updateSocketManager: UpdateSocketManager

    override fun start(startPromise: Promise<Void>) {
        val router = Router.router(vertx)
        router.route().handler(BodyHandler.create())

        // Set up core WebSocket routes
        setupCoreRoutes(router)

        // Allow implementers to set up additional routes
        setupApplicationRoutes(router)

        // Start HTTP server
        val httpServer = vertx.createHttpServer()
        httpServer.requestHandler(router)
            .listen(getServerPort()) { result ->
                if (result.succeeded()) {
                    log.info("Application started on port {}", getServerPort())

                    // Call onStart hook for application-specific initialization
                    onStart()
                        .onSuccess { startPromise.complete() }
                        .onFailure { err ->
                            log.error("Failed during application initialization", err)
                            startPromise.fail(err)
                        }
                } else {
                    log.error("Failed to start application", result.cause())
                    startPromise.fail(result.cause())
                }
            }
    }

    // Hook for application-specific initialization
    protected open fun onStart(): Future<Void> {
        // Default implementation does nothing
        return Future.succeededFuture()
    }

    private fun setupCoreRoutes(router: Router) {
        // Command socket for receiving client requests
        router.route("/ws").handler { context ->
            val request = context.request()
            request.toWebSocket { websocketResult ->
                if (websocketResult.succeeded()) {
                    commandSocketManager.handle(websocketResult.result())
                }
            }
        }

        // Update socket for pushing changes to clients
        router.route("/ws/updates").handler { context ->
            val request = context.request()
            request.toWebSocket { websocketResult ->
                if (websocketResult.succeeded()) {
                    updateSocketManager.handle(websocketResult.result())
                }
            }
        }
    }

    // Override this to add application-specific routes
    protected open fun setupApplicationRoutes(router: Router) {
        // No default implementation
    }

    // Override to customize port
    protected open fun getServerPort(): Int {
        return 8080
    }
}