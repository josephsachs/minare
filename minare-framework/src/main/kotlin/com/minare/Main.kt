package com.minare

import com.google.inject.Guice
import com.minare.config.GuiceModule
import com.minare.core.state.MongoChangeStreamConsumer
import com.minare.core.websocket.WebSocketRoutes
import com.minare.persistence.DatabaseInitializer
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import org.slf4j.LoggerFactory

/**
 * Main application entry point.
 * Initializes the dependency injection container and starts the server.
 */
class Main {
    companion object {
        private val log = LoggerFactory.getLogger(Main::class.java)

        @JvmStatic
        fun main(args: Array<String>) {
            val injector = Guice.createInjector(GuiceModule())

            val vertx = injector.getInstance(Vertx::class.java)
            val wsRoutes = injector.getInstance(WebSocketRoutes::class.java)
            val router = Router.router(vertx)

            val dbInitializer = injector.getInstance(DatabaseInitializer::class.java)
            val changeStreamConsumer = injector.getInstance(MongoChangeStreamConsumer::class.java)

            dbInitializer.initialize()
                .compose<Nothing?> {
                    changeStreamConsumer.startConsuming()
                    wsRoutes.register(router)

                    // Start the HTTP server
                    vertx.createHttpServer()
                        .requestHandler(router)
                        .listen(8080)
                        .onSuccess { http -> log.info("Server started on port 8080") }
                        .onFailure { err ->
                            log.error("Failed to start server", err)
                            System.exit(1)
                        }
                        .mapEmpty()
                }
        }
    }
}