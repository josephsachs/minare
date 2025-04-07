package com.minare

import com.google.inject.*
import com.google.inject.name.Names
import com.minare.config.*
import com.minare.controller.ConnectionController
import com.minare.worker.ChangeStreamWorkerVerticle
import com.minare.worker.command.CommandVerticle
import com.minare.worker.CleanupVerticle
import com.minare.worker.MutationVerticle
import com.minare.worker.update.UpdateVerticle
import com.minare.persistence.DatabaseInitializer
import com.minare.utils.EventBusUtils
import com.minare.utils.VerticleLogger
import com.minare.worker.MinareVerticleFactory
import com.minare.worker.command.config.CommandVerticleModule
import com.minare.worker.update.config.UpdateVerticleModule
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.http.HttpServer
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import io.vertx.spi.cluster.hazelcast.HazelcastClusterManager
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import kotlin.system.exitProcess

/**
 * Base application class that provides the core framework functionality.
 * Applications should extend this class and implement the abstract methods.
 */
abstract class MinareApplication : CoroutineVerticle() {
    private val log = LoggerFactory.getLogger(MinareApplication::class.java)
    @Inject
    private lateinit var connectionController: ConnectionController
    @Inject
    lateinit var databaseInitializer: DatabaseInitializer
    @Inject
    lateinit var injector: Injector

    // Track the deployment IDs of our worker verticles
    private var changeStreamWorkerDeploymentId: String? = null
    private var mutationVerticleDeploymentId: String? = null
    private var commandVerticleDeploymentId: String? = null
    private var cleanupVerticleDeploymentId: String? = null
    private var updateVerticleDeploymentId: String? = null

    private val pendingConnections = ConcurrentHashMap<String, ConnectionState>()

    private class ConnectionState {
        var commandSocketConnected = false
        var updateSocketConnected = false
        var traceId: String? = null
    }

    // Main HTTP server
    var httpServer: HttpServer? = null

    object ConnectionEvents {
        const val ADDRESS_COMMAND_SOCKET_CONNECTED = "minare.connection.command.connected"
        const val ADDRESS_UPDATE_SOCKET_CONNECTED = "minare.connection.update.connected"
        const val ADDRESS_CONNECTION_COMPLETE = "minare.connection.complete"
    }

    /**
     * Main startup method. Initializes the dependency injection,
     * database, and starts the server with WebSocket routes.
     */
    override suspend fun start() {
        try {
            databaseInitializer.initialize()
            log.info("Database initialized successfully")

            val processorCount = Runtime.getRuntime().availableProcessors()

            vertx.registerVerticleFactory(MinareVerticleFactory(injector))
            log.info("Registered MinareVerticleFactory")

            val commandSocketOptions = DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("command-socket-pool")
                .setWorkerPoolSize(5)
                .setInstances(3)
                .setConfig(JsonObject().put("useOwnHttpServer", true))

            commandVerticleDeploymentId = vertx.deployVerticle(
                "guice:" + CommandVerticle::class.java.name,
                commandSocketOptions
            ).await()
            log.info("Command socket verticle deployed with ID: $commandVerticleDeploymentId")

            // Deploy the UpdateVerticle for frame-based update processing
            val updateVerticleOptions = DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("update-processor-pool")
                .setWorkerPoolSize(7)  // Adjust based on needs
                .setInstances(7)  // Single instance to centralize update processing

            updateVerticleDeploymentId = vertx.deployVerticle(
                "guice:" + UpdateVerticle::class.java.name,
                updateVerticleOptions
            ).await()
            log.info("Update verticle deployed with ID: $updateVerticleDeploymentId")

            // Deploy the change stream worker verticle as a worker verticle
            val changeStreamOptions = DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("change-stream-pool")
                .setWorkerPoolSize(2)
                .setInstances(2)
                .setMaxWorkerExecuteTime(Long.MAX_VALUE)  // Allow long-running tasks

            // Deploy using the GuiceVerticleFactory
            changeStreamWorkerDeploymentId = vertx.deployVerticle(
                "guice:" + ChangeStreamWorkerVerticle::class.java.name,
                changeStreamOptions
            ).await()
            log.info("Change stream worker deployed with ID: $changeStreamWorkerDeploymentId")

            val mutationOptions = DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("mutation-pool")
                .setWorkerPoolSize(2)
                .setInstances(1)

            // Deploy using the GuiceVerticleFactory
            mutationVerticleDeploymentId = vertx.deployVerticle(
                "guice:" + MutationVerticle::class.java.name,
                mutationOptions
            ).await()
            log.info("Mutation verticle deployed with ID: $mutationVerticleDeploymentId")

            // Deploy the cleanup verticle
            val cleanupOptions = DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("cleanup-pool")
                .setWorkerPoolSize(1)  // Only need one worker
                .setInstances(1)

            cleanupVerticleDeploymentId = vertx.deployVerticle(
                "guice:" + CleanupVerticle::class.java.name,
                cleanupOptions
            ).await()
            log.info("Cleanup verticle deployed with ID: $cleanupVerticleDeploymentId")

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

            // Initialize the CommandSocketVerticle router
            val initResult = vertx.eventBus().request<JsonObject>(
                CommandVerticle.ADDRESS_COMMAND_SOCKET_INITIALIZE,
                JsonObject()
            ).await()

            if (initResult.body().getBoolean("success", false)) {
                log.info("CommandSocketVerticle router initialized: {}",
                    initResult.body().getString("message"))
            } else {
                log.error("Failed to initialize CommandSocketVerticle router")
            }

            // Register application connection events
            registerConnectionEventHandlers()

            // Let implementing class add custom routes
            setupApplicationRoutes()

            // Call application-specific initialization
            onApplicationStart()

        } catch (e: Exception) {
            log.error("Failed to start application", e)
            throw e
        }
    }

    /**
     * Override to add application-specific HTTP routes.
     */
    protected open suspend fun setupApplicationRoutes() {
        // Default implementation does nothing
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
            // Undeploy the command socket verticle
            if (commandVerticleDeploymentId != null) {
                try {
                    vertx.undeploy(commandVerticleDeploymentId).await()
                    log.info("Command socket verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying command socket verticle", e)
                }
            }

            // Undeploy the update verticle
            if (updateVerticleDeploymentId != null) {
                try {
                    vertx.undeploy(updateVerticleDeploymentId).await()
                    log.info("Update verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying update verticle", e)
                }
            }

            // Undeploy the mutation verticle
            if (mutationVerticleDeploymentId != null) {
                try {
                    vertx.undeploy(mutationVerticleDeploymentId).await()
                    log.info("Mutation verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying mutation verticle", e)
                }
            }

            // Undeploy the change stream worker verticle
            if (changeStreamWorkerDeploymentId != null) {
                try {
                    vertx.undeploy(changeStreamWorkerDeploymentId).await()
                    log.info("Change stream worker undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying change stream worker", e)
                }
            }

            // Undeploy the cleanup verticle
            if (cleanupVerticleDeploymentId != null) {
                try {
                    vertx.undeploy(cleanupVerticleDeploymentId).await()
                    log.info("Cleanup verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying cleanup verticle", e)
                }
            }

            // Close HTTP server
            if (httpServer != null) {
                try {
                    httpServer?.close()?.await()
                    log.info("HTTP server closed successfully")
                } catch (e: Exception) {
                    log.error("Error closing HTTP server", e)
                }
            }

            log.info("Application stopped gracefully")
        } catch (e: Exception) {
            log.error("Error during application shutdown", e)
            throw e
        }
    }

    private fun registerConnectionEventHandlers() {
        // Listen for command socket connections
        vertx.eventBus().consumer<JsonObject>(ConnectionEvents.ADDRESS_COMMAND_SOCKET_CONNECTED) { message ->
            val connectionId = message.body().getString("connectionId")
            val traceId = message.body().getString("traceId")

            Companion.log.info("MinareApplication acknowledges command socket for ${connectionId}")

            if (connectionId != null) {
                CoroutineScope(vertx.dispatcher()).launch {
                    handleCommandSocketConnected(connectionId, traceId)
                }
            }
        }

        // Listen for update socket connections
        vertx.eventBus().consumer<JsonObject>(ConnectionEvents.ADDRESS_UPDATE_SOCKET_CONNECTED) { message ->
            val connectionId = message.body().getString("connectionId")
            val traceId = message.body().getString("traceId")

            Companion.log.info("MinareApplication acknowledges update socket for ${connectionId}")

            if (connectionId != null) {
                CoroutineScope(vertx.dispatcher()).launch {
                    handleUpdateSocketConnected(connectionId, traceId)
                }
            }
        }

        Companion.log.info("Connection event handlers registered")
    }

    private suspend fun handleCommandSocketConnected(connectionId: String, traceId: String?) {
        val state = pendingConnections.computeIfAbsent(connectionId) { ConnectionState() }
        state.commandSocketConnected = true
        if (traceId != null) state.traceId = traceId

        checkConnectionComplete(connectionId)
    }

    private suspend fun handleUpdateSocketConnected(connectionId: String, traceId: String?) {
        val state = pendingConnections.computeIfAbsent(connectionId) { ConnectionState() }
        state.updateSocketConnected = true
        if (traceId != null) state.traceId = traceId

        checkConnectionComplete(connectionId)
    }

    private suspend fun checkConnectionComplete(connectionId: String) {
        val state = pendingConnections[connectionId] ?: return

        if (state.commandSocketConnected && state.updateSocketConnected) {
            // Connection is complete
            Companion.log.info("Connection $connectionId is now fully established")

            try {
                // Get the connection object
                val connection = connectionController.getConnection(connectionId)

                // Trigger the application-specific handler
                connectionController.onClientFullyConnected(connection)

                // Notify others that might be interested
                vertx.eventBus().publish(
                    ConnectionEvents.ADDRESS_CONNECTION_COMPLETE,
                    JsonObject()
                        .put("connectionId", connectionId)
                        .put("traceId", state.traceId)
                )

                // Remove from pending connections
                pendingConnections.remove(connectionId)
            } catch (e: Exception) {
                Companion.log.error("Error handling connection completion for $connectionId", e)
            }
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
         * Check if we should enable clustering
         */
        private fun isClusteringEnabled(args: Array<String>): Boolean {
            return args.contains("--cluster") ||
                    System.getenv("ENABLE_CLUSTERING")?.equals("true", ignoreCase = true) == true
        }

        /**
         * Start the application with the given implementation class.
         * This is the main entry point that applications should use.
         */
        fun start(applicationClass: Class<out MinareApplication>, args: Array<String>) {
            // Check if clustering is enabled
            val clusteringEnabled = isClusteringEnabled(args)

            // Create Vertx options with optional clustering
            val vertxOptions = VertxOptions()

            if (clusteringEnabled) {
                log.info("Clustering is enabled")

                // Set up Hazelcast cluster manager
                val clusterManager = HazelcastClusterManager()
                vertxOptions.clusterManager = clusterManager

                // Start clustered Vertx instance
                Vertx.clusteredVertx(vertxOptions).onComplete { ar ->
                    if (ar.succeeded()) {
                        val vertx = ar.result()
                        log.info("Successfully created clustered Vertx instance")
                        completeStartup(vertx, applicationClass, args, clusteringEnabled)
                    } else {
                        log.error("Failed to create clustered Vertx instance", ar.cause())
                        exitProcess(1)
                    }
                }
            } else {
                // Start standard Vertx instance
                val vertx = Vertx.vertx(vertxOptions)
                completeStartup(vertx, applicationClass, args, false)
            }
        }

        /**
         * Complete the startup process once Vertx is initialized
         */
        private fun completeStartup(
            vertx: Vertx,
            applicationClass: Class<out MinareApplication>,
            args: Array<String>,
            clusteringEnabled: Boolean
        ) {
            try {
                // Get application module from the application class
                val appModule = getApplicationModule(applicationClass)
                log.info("Loaded application module: ${appModule.javaClass.name}")

                // Get database name from the application module
                val dbName = getDatabaseNameFromModule(appModule)
                log.info("Using database name: $dbName")

                // Create the framework module
                val frameworkModule = MinareModule()
                val commandVerticleModule = CommandVerticleModule()
                val updateVerticleModule = UpdateVerticleModule()

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

                        // Bind clustering configuration
                        bind(Boolean::class.java)
                            .annotatedWith(Names.named("clusteringEnabled"))
                            .toInstance(clusteringEnabled)
                    }
                }

                // Create a single combined module to avoid binding conflicts
                val combinedModule = object : AbstractModule() {
                    override fun configure() {
                        // Install modules in correct order:
                        // 1. First framework (provides defaults)
                        install(frameworkModule)
                        install(commandVerticleModule)
                        install(updateVerticleModule)
                        // 2. Then app module (overrides framework if needed)
                        install(appModule)
                        // 3. Then vertx and database modules
                        install(vertxModule)
                        install(dbNameModule)
                    }
                }

                val injector = Guice.createInjector(combinedModule) // Create injector with combined module
                val app = injector.getInstance(applicationClass) // Get a properly instantiated application instance with all dependencies
                InternalInjectorHolder.setInjector(injector) // Store injector reference in a static field if needed

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