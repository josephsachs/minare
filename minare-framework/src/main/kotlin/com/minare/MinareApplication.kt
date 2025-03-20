package com.minare

import com.google.inject.*
import com.google.inject.name.Names
import com.minare.config.DatabaseNameProvider
import com.minare.config.GuiceModule
import com.minare.config.InternalInjectorHolder
import com.minare.core.state.ChangeStreamWorkerVerticle
import com.minare.core.websocket.CommandSocketManager
import com.minare.core.websocket.UpdateSocketManager
import com.minare.persistence.DatabaseInitializer
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.json.JsonObject
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import kotlin.system.exitProcess

/**
 * Base application class that provides the core framework functionality.
 * Applications should extend this class and implement the abstract methods.
 */
abstract class MinareApplication : CoroutineVerticle() {
    private val log = LoggerFactory.getLogger(MinareApplication::class.java)

    @Inject
    lateinit var commandSocketManager: CommandSocketManager

    @Inject
    lateinit var updateSocketManager: UpdateSocketManager

    @Inject
    lateinit var databaseInitializer: DatabaseInitializer

    @Inject
    lateinit var changeStreamWorkerVerticle: ChangeStreamWorkerVerticle

    // Guice injector for dependency management (not needed to be injected anymore)
    // We'll use InternalInjectorHolder instead if we need access to the injector

    // Track the deployment ID of our worker verticle
    private var changeStreamWorkerDeploymentId: String? = null

    /**
     * Main startup method. Initializes the dependency injection,
     * database, and starts the server with WebSocket routes.
     */
    override suspend fun start() {
        try {
            // Initialize database
            databaseInitializer.initialize()
            log.info("Database initialized successfully")

            // Deploy the change stream worker verticle as a worker verticle
            val options = DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("change-stream-pool")
                .setWorkerPoolSize(1)  // We only need one worker for the change stream
                .setMaxWorkerExecuteTime(Long.MAX_VALUE)  // Allow long-running tasks

            changeStreamWorkerDeploymentId = vertx.deployVerticle(changeStreamWorkerVerticle, options).await()
            log.info("Change stream worker deployed with ID: $changeStreamWorkerDeploymentId")

            // Register event bus handlers for change stream events
            vertx.eventBus().consumer<Boolean>(ChangeStreamWorkerVerticle.ADDRESS_STREAM_STARTED) {
                log.info("Received stream started notification")
            }

            vertx.eventBus().consumer<Boolean>(ChangeStreamWorkerVerticle.ADDRESS_STREAM_STOPPED) {
                log.info("Received stream stopped notification")
            }

            vertx.eventBus().consumer<JsonObject>(ChangeStreamWorkerVerticle.ADDRESS_ENTITY_UPDATED) { message ->
                log.debug("Received entity update notification: ${message.body()}")
            }

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
            // Undeploy the change stream worker verticle
            if (changeStreamWorkerDeploymentId != null) {
                try {
                    vertx.undeploy(changeStreamWorkerDeploymentId).await()
                    log.info("Change stream worker undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying change stream worker", e)
                }
            }

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
         * Get the application module for a specific application class
         */
        private fun getApplicationModule(applicationClass: Class<out MinareApplication>): Module {
            // First check if the class has a static getModule method
            try {
                val getModuleMethod = applicationClass.getDeclaredMethod("getModule")
                getModuleMethod.isAccessible = true
                return getModuleMethod.invoke(null) as Module
            } catch (e: Exception) {
                // Fallback to a default empty module
                log.warn("No getModule() method found in application class ${applicationClass.name}. Using empty module.")
                return Module { /* Empty module */ }
            }
        }

        /**
         * Extract database name from the application module if available.
         * Uses the DatabaseNameProvider interface if implemented, otherwise returns a default.
         */
        private fun getDatabaseNameFromModule(module: Module): String {
            return if (module is DatabaseNameProvider) {
                val dbName = module.getDatabaseName()
                log.info("Using database name from DatabaseNameProvider: $dbName")
                dbName
            } else {
                log.info("Module doesn't implement DatabaseNameProvider, using default database name")
                "minare" // Default database name
            }
        }

        /**
         * Start the application with the given implementation class.
         * This is the main entry point that applications should use.
         */
        fun start(applicationClass: Class<out MinareApplication>, args: Array<String>) {
            val vertx = Vertx.vertx()

            try {
                // Get application module from the application class
                val appModule = getApplicationModule(applicationClass)
                log.info("Loaded application module: ${appModule.javaClass.name}")

                // Get database name from the application module
                val dbName = getDatabaseNameFromModule(appModule)
                log.info("Using database name: $dbName")

                // Create the framework module
                val frameworkModule = GuiceModule()

                // Create a module for database name binding
                val dbNameModule = object : AbstractModule() {
                    override fun configure() {
                        bind(String::class.java)
                            .annotatedWith(Names.named("databaseName"))
                            .toInstance(dbName)
                    }
                }

                // Create a module that binds Vertx
                val vertxModule = object : AbstractModule() {
                    override fun configure() {
                        bind(Vertx::class.java).toInstance(vertx)
                    }
                }

                // Create a single combined module to avoid binding conflicts
                val combinedModule = object : AbstractModule() {
                    override fun configure() {
                        // Install modules in correct order:
                        // 1. First framework (provides defaults)
                        install(frameworkModule)
                        // 2. Then app module (overrides framework if needed)
                        install(appModule)
                        // 3. Then vertx and database modules
                        install(vertxModule)
                        install(dbNameModule)
                    }
                }

                // Create injector with combined module
                val injector = Guice.createInjector(combinedModule)

                // Get a properly instantiated application instance with all dependencies
                val app = injector.getInstance(applicationClass)

                // Store injector reference in a static field if needed
                InternalInjectorHolder.setInjector(injector)

                // Deploy the verticle with proper coroutine context
                vertx.deployVerticle(app)
                    .onSuccess { deploymentId ->
                        log.info("Application deployed successfully with ID: $deploymentId")
                    }
                    .onFailure { error ->
                        log.error("Failed to deploy application", error)
                        vertx.close()
                        exitProcess(1)
                    }

                // Add shutdown hook for clean termination
                Runtime.getRuntime().addShutdownHook(Thread {
                    log.info("Shutting down application...")
                    // Use runBlocking to properly wait for coroutines to complete during shutdown
                    runBlocking {
                        try {
                            // Signal the verticle to stop
                            vertx.undeploy(app.deploymentID)
                                .onComplete {
                                    vertx.close()
                                    log.info("Application shutdown complete")
                                }
                        } catch (e: Exception) {
                            log.error("Error during shutdown", e)
                            vertx.close()
                        }
                    }
                })

            } catch (e: Exception) {
                log.error("Failed to start application", e)
                vertx.close()
                exitProcess(1)
            }
        }
    }
}