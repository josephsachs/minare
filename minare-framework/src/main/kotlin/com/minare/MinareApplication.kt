package com.minare

import com.google.inject.Guice
import com.google.inject.Inject
import com.google.inject.Injector
import com.minare.config.GuiceModule
import com.google.inject.Module
import com.minare.core.state.MongoEntityStreamConsumer
import com.minare.core.websocket.CommandSocketManager
import com.minare.core.websocket.UpdateSocketManager
import com.minare.persistence.DatabaseInitializer
import io.vertx.core.Vertx
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory

/**
 * Base application class that provides the core framework functionality.
 * Applications should extend this class and implement the abstract methods.
 */
abstract class MinareApplication : CoroutineVerticle() {
    private val log = LoggerFactory.getLogger(MinareApplication::class.java)

    @Inject
    lateinit var commandSocketManager: CommandSocketManager
    @Inject lateinit var updateSocketManager: UpdateSocketManager
    @Inject lateinit var databaseInitializer: DatabaseInitializer
    @Inject lateinit var entityStreamConsumer: MongoEntityStreamConsumer

    // Guice injector for dependency management
    protected lateinit var injector: Injector

    /**
     * Main startup method. Initializes the dependency injection,
     * database, and starts the server with WebSocket routes.
     */
    override suspend fun start() {
        try {
            // Initialize dependency injection
            injector = createInjector()
            injector.injectMembers(this)

            // Initialize database
            databaseInitializer.initialize().await()
            log.info("Database initialized successfully")

            // Start entity change stream listener
            entityStreamConsumer.listen()
            log.info("Entity change stream listener started")

            // Set up HTTP server and routes
            val router = Router.router(vertx)
            router.route().handler(BodyHandler.create())

            // Set up WebSocket routes
            setupCoreRoutes(router)

            // Let implementing class add custom routes
            setupApplicationRoutes(router)

            // Start HTTP server
            val serverPort = getServerPort()
            val server = vertx.createHttpServer()
                .requestHandler(router)
                .listen(serverPort)
                .await()

            log.info("Server started on port {}", serverPort)

            // Call application-specific initialization
            onApplicationStart()

        } catch (e: Exception) {
            log.error("Failed to start application", e)
            throw e
        }
    }

    /**
     * Override to create a custom injector if needed.
     * Default implementation creates an injector with the standard GuiceModule
     * and allows the implementer to provide an additional application-specific module.
     */
    protected open fun createInjector(): Injector {
        return Guice.createInjector(
            GuiceModule(),                      // Core framework module
            createApplicationModule()          // Application-specific module
        )
    }


    /**
     * Create an application-specific Guice module for dependency injection.
     * Override this to provide custom services and configurations.
     * For example, return your com.minare.example.config.GuiceModule
     */
    protected open fun createApplicationModule(): Module {
        return Module { /* Empty default module */ }
    }

    /**
     * Setup core WebSocket routes for command and update sockets.
     */
    private fun setupCoreRoutes(router: Router) {
        // Command socket route for client requests
        router.route("/ws").handler { context ->
            context.request().toWebSocket()
                .onSuccess(commandSocketManager)
                .onFailure { err ->
                    log.error("Command WebSocket upgrade failed", err)
                    context.response()
                        .setStatusCode(400)
                        .end("Command WebSocket upgrade failed")
                }
        }

        // Update socket route for server updates
        router.route("/ws/updates").handler { context ->
            context.request().toWebSocket()
                .onSuccess(updateSocketManager)
                .onFailure { err ->
                    log.error("Update WebSocket upgrade failed", err)
                    context.response()
                        .setStatusCode(400)
                        .end("Update WebSocket upgrade failed")
                }
        }
    }

    /**
     * Override to add application-specific HTTP routes.
     */
    protected open fun setupApplicationRoutes(router: Router) {
        // Default implementation does nothing
    }

    /**
     * Override to customize the server port.
     * Default is 8080.
     */
    protected open fun getServerPort(): Int {
        return 8080
    }

    /**
     * Application-specific initialization logic.
     * Called after the server has started successfully.
     */
    protected open suspend fun onApplicationStart() {
        // Default implementation does nothing
    }

    /**
     * Helper method for clean shutdown
     */
    override suspend fun stop() {
        try {
            // Stop entity change stream
            entityStreamConsumer.stopListening()
            log.info("Entity change stream listener stopped")

            // Add other cleanup tasks here

            log.info("Application stopped gracefully")
        } catch (e: Exception) {
            log.error("Error during application shutdown", e)
            throw e
        }
    }

    /**
     * Main entry point that starts the application.
     * Implementations can use this in their main function.
     */
    companion object {
        private val log = LoggerFactory.getLogger(MinareApplication::class.java)

        /**
         * Start the application with the given implementation class.
         */
        fun start(applicationClass: Class<out MinareApplication>, args: Array<String>) {
            val vertx = Vertx.vertx()

            try {
                // Create and deploy the verticle
                vertx.deployVerticle(applicationClass.newInstance())
                    .onFailure { err ->
                        log.error("Failed to deploy application", err)
                        vertx.close()
                        System.exit(1)
                    }

                // Add shutdown hook for clean termination
                Runtime.getRuntime().addShutdownHook(Thread {
                    vertx.close()
                    log.info("Vertx context closed")
                })

            } catch (e: Exception) {
                log.error("Failed to start application", e)
                vertx.close()
                System.exit(1)
            }
        }
    }
}