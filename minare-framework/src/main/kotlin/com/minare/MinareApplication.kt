package com.minare

import com.google.inject.*
import com.google.inject.name.Names
import com.minare.config.*
import com.minare.controller.ConnectionController
import com.minare.worker.command.CommandVerticle
import com.minare.worker.update.UpdateVerticle
import com.minare.persistence.DatabaseInitializer
import com.minare.worker.*
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

            val updateVerticleOptions = DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("update-processor-pool")
                .setWorkerPoolSize(7)
                .setInstances(7)

            updateVerticleDeploymentId = vertx.deployVerticle(
                "guice:" + UpdateVerticle::class.java.name,
                updateVerticleOptions
            ).await()
            log.info("Update verticle deployed with ID: $updateVerticleDeploymentId")

            val changeStreamOptions = DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("redis-pubsub-pool")
                .setWorkerPoolSize(2)
                .setInstances(2)
                .setMaxWorkerExecuteTime(Long.MAX_VALUE)

            changeStreamWorkerDeploymentId = vertx.deployVerticle(
                "guice:" + RedisPubSubWorkerVerticle::class.java.name,
                changeStreamOptions
            ).await()
            log.info("Redis pub/sub worker deployed with ID: $changeStreamWorkerDeploymentId")

            val mutationOptions = DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("mutation-pool")
                .setWorkerPoolSize(2)
                .setInstances(1)

            mutationVerticleDeploymentId = vertx.deployVerticle(
                "guice:" + MutationVerticle::class.java.name,
                mutationOptions
            ).await()
            log.info("Mutation verticle deployed with ID: $mutationVerticleDeploymentId")

            val cleanupOptions = DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("cleanup-pool")
                .setWorkerPoolSize(1)
                .setInstances(1)

            cleanupVerticleDeploymentId = vertx.deployVerticle(
                "guice:" + CleanupVerticle::class.java.name,
                cleanupOptions
            ).await()
            log.info("Cleanup verticle deployed with ID: $cleanupVerticleDeploymentId")

            vertx.eventBus().consumer<Boolean>(ChangeStreamWorkerVerticle.ADDRESS_STREAM_STARTED) {
                log.info("Received stream started notification")
            }

            vertx.eventBus().consumer<Boolean>(ChangeStreamWorkerVerticle.ADDRESS_STREAM_STOPPED) {
                log.info("Received stream stopped notification")
            }

            vertx.eventBus().consumer<JsonObject>(ChangeStreamWorkerVerticle.ADDRESS_ENTITY_UPDATED) { message ->
                log.debug("Received entity update notification: ${message.body()}")
            }

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

            registerConnectionEventHandlers()

            setupApplicationRoutes()

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

    }

    /**
     * Application-specific initialization logic.
     * Called after the server has started successfully.
     */
    protected open suspend fun onApplicationStart() {

    }

    /**
     * Helper method for clean shutdown
     */
    override suspend fun stop() {
        try {

            if (commandVerticleDeploymentId != null) {
                try {
                    vertx.undeploy(commandVerticleDeploymentId).await()
                    log.info("Command socket verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying command socket verticle", e)
                }
            }


            if (updateVerticleDeploymentId != null) {
                try {
                    vertx.undeploy(updateVerticleDeploymentId).await()
                    log.info("Update verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying update verticle", e)
                }
            }


            if (mutationVerticleDeploymentId != null) {
                try {
                    vertx.undeploy(mutationVerticleDeploymentId).await()
                    log.info("Mutation verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying mutation verticle", e)
                }
            }


            if (changeStreamWorkerDeploymentId != null) {
                try {
                    vertx.undeploy(changeStreamWorkerDeploymentId).await()
                    log.info("Change stream worker undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying change stream worker", e)
                }
            }


            if (cleanupVerticleDeploymentId != null) {
                try {
                    vertx.undeploy(cleanupVerticleDeploymentId).await()
                    log.info("Cleanup verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying cleanup verticle", e)
                }
            }


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
            Companion.log.info("Connection $connectionId is now fully established")

            try {
                val connection = connectionController.getConnection(connectionId)

                connectionController.onClientFullyConnected(connection)

                vertx.eventBus().publish(
                    ConnectionEvents.ADDRESS_CONNECTION_COMPLETE,
                    JsonObject()
                        .put("connectionId", connectionId)
                        .put("traceId", state.traceId)
                )

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

            try {
                val getModuleMethod = applicationClass.getDeclaredMethod("getModule")
                getModuleMethod.isAccessible = true
                return getModuleMethod.invoke(null) as Module
            } catch (e: Exception) {

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
                "minare"
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
            val clusteringEnabled = isClusteringEnabled(args)

            val vertxOptions = VertxOptions()

            if (clusteringEnabled) {
                log.info("Clustering is enabled")

                val clusterManager = HazelcastClusterManager()
                vertxOptions.clusterManager = clusterManager

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
                val appModule = getApplicationModule(applicationClass)
                log.info("Loaded application module: ${appModule.javaClass.name}")

                val dbName = getDatabaseNameFromModule(appModule)
                log.info("Using database name: $dbName")

                val frameworkModule = MinareModule()
                val commandVerticleModule = CommandVerticleModule()
                val updateVerticleModule = UpdateVerticleModule()

                val dbNameModule = object : AbstractModule() {
                    override fun configure() {
                        bind(String::class.java)
                            .annotatedWith(Names.named("databaseName"))
                            .toInstance(dbName)
                    }
                }

                val vertxModule = object : AbstractModule() {
                    override fun configure() {
                        bind(Vertx::class.java).toInstance(vertx)
                        bind(Boolean::class.java)
                            .annotatedWith(Names.named("clusteringEnabled"))
                            .toInstance(clusteringEnabled)
                    }
                }

                val combinedModule = object : AbstractModule() {
                    override fun configure() {
                        // Correct order is required:
                        // framework (provides defaults)
                        install(frameworkModule)
                        install(commandVerticleModule)
                        install(updateVerticleModule)
                        // Then app module (overrides framework if needed)
                        install(appModule)
                        // Then vertx and database modules
                        install(vertxModule)
                        install(dbNameModule)
                    }
                }

                val injector = Guice.createInjector(combinedModule)
                val app = injector.getInstance(applicationClass)
                InternalInjectorHolder.setInjector(injector)

                vertx.deployVerticle(app)
                    .onSuccess { deploymentId ->
                        log.info("Application deployed successfully with ID: $deploymentId")
                    }
                    .onFailure { error ->
                        log.error("Failed to deploy application", error)
                        vertx.close()
                        exitProcess(1)
                    }

                Runtime.getRuntime().addShutdownHook(Thread {
                    log.info("Shutting down application...")
                    runBlocking {
                        try {
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