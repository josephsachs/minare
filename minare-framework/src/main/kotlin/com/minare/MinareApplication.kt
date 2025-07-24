package com.minare

import com.google.inject.*
import com.google.inject.name.Names
import com.minare.MinareApplication.ConnectionEvents.ADDRESS_COORDINATOR_STARTED
import com.minare.MinareApplication.ConnectionEvents.ADDRESS_WORKER_STARTED
import com.minare.config.*
import com.minare.controller.ConnectionController
import com.minare.worker.coordinator.FrameCoordinatorVerticle
import com.minare.worker.upsocket.UpSocketVerticle
import com.minare.worker.downsocket.DownSocketVerticle
import com.minare.persistence.DatabaseInitializer
import com.minare.time.TimeService
import com.minare.worker.*
import com.minare.worker.coordinator.CoordinatorAdminVerticle
import com.minare.worker.coordinator.FrameWorkerVerticle
import com.minare.worker.coordinator.config.FrameCoordinatorVerticleModule
import com.minare.worker.downsocket.config.DownSocketVerticleModule
import com.minare.worker.upsocket.config.UpSocketVerticleModule
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.http.HttpServer
import io.vertx.core.json.JsonArray
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
    private lateinit var timeService: TimeService
    @Inject
    lateinit var databaseInitializer: DatabaseInitializer
    @Inject
    lateinit var injector: Injector

    private var processorCount: Number? = null
    private var frameCoordinatorVerticleDeploymentId: String? = null
    private var mutationVerticleDeploymentId: String? = null
    private var upSocketVerticleDeploymentId: String? = null
    private var redisPubSubWorkerDeploymentId: String? = null
    private var cleanupVerticleDeploymentId: String? = null
    private var downSocketVerticleDeploymentId: String? = null
    private var messageQueueConsumerVerticleDeploymentId: String? = null

    private val pendingConnections = ConcurrentHashMap<String, ConnectionState>()

    private class ConnectionState {
        var upSocketConnected = false
        var downSocketConnected = false
        var traceId: String? = null
    }

    var httpServer: HttpServer? = null

    object ConnectionEvents {
        const val ADDRESS_UP_SOCKET_CONNECTED = "minare.connection.up.connected"
        const val ADDRESS_DOWN_SOCKET_CONNECTED = "minare.connection.down.connected"
        const val ADDRESS_CONNECTION_COMPLETE = "minare.connection.complete"

        const val ADDRESS_COORDINATOR_STARTED = "minare.cluster.coordinator.started"
        const val ADDRESS_WORKER_STARTED = "minare.cluster.worker.started"
    }

    /**
     * Main startup method. Initializes the dependency injection,
     * database, and starts the server with WebSocket routes.
     */
    override suspend fun start() {
        try {
            timeService.syncTime()
            log.info("Time synchronization complete")

            databaseInitializer.initialize()
            log.info("Database initialized successfully")

            processorCount = Runtime.getRuntime().availableProcessors()

            val instanceRole = System.getenv("INSTANCE_ROLE") ?:
                throw IllegalStateException("INSTANCE_ROLE environment variable is required")

            vertx.registerVerticleFactory(MinareVerticleFactory(injector))
            log.info("Registered MinareVerticleFactory")

            when (instanceRole) {
                "COORDINATOR" -> {
                    initializeCoordinator()
                }
                "WORKER" -> {
                    initializeWorker()
                }
            }

        } catch (e: Exception) {
            log.error("Failed to start application", e)
            throw e
        }
    }

    /**
     * Initialize the coordinator
     */
    private suspend fun initializeCoordinator() {
        val frameCordinatorVerticleOptions = DeploymentOptions()
            .setInstances(1)
            .setConfig(
                JsonObject()
                    .put("role", "coordinator")
            )

        frameCoordinatorVerticleDeploymentId = vertx.deployVerticle(
            "guice:" + FrameCoordinatorVerticle::class.java.name,
            frameCordinatorVerticleOptions
        ).await()

        vertx.eventBus().publish(ADDRESS_COORDINATOR_STARTED, JsonObject())
        log.info("Coordinator verticle deployed with ID: $frameCoordinatorVerticleDeploymentId")

        val coordinatorAdminOptions = DeploymentOptions()
            .setInstances(1)
            .setConfig(JsonObject().put("role", "coordinator-admin"))

        val coordinatorAdminDeploymentId = vertx.deployVerticle(
            "guice:" + CoordinatorAdminVerticle::class.java.name,
            coordinatorAdminOptions
        ).await()
        log.info("Coordinator admin interface deployed with ID: {} on port 9090", coordinatorAdminDeploymentId)
    }

    /**
     * Initialize a worker instance
     */
    private suspend fun initializeWorker() {
        val upSocketVerticleOptions = DeploymentOptions()
            .setWorker(true)
            .setWorkerPoolName("up-socket-pool")
            .setWorkerPoolSize(5)
            .setInstances(3)
            .setConfig(JsonObject().put("useOwnHttpServer", true))

        upSocketVerticleDeploymentId = vertx.deployVerticle(
            "guice:" + UpSocketVerticle::class.java.name,
            upSocketVerticleOptions
        ).await()
        log.info("Up socket verticle deployed with ID: $upSocketVerticleDeploymentId")

        val downSocketVerticleOptions = DeploymentOptions()
            .setWorker(true)
            .setWorkerPoolName("down-socket-pool")
            .setWorkerPoolSize(7)
            .setInstances(7)

        downSocketVerticleDeploymentId = vertx.deployVerticle(
            "guice:" + DownSocketVerticle::class.java.name,
            downSocketVerticleOptions
        ).await()
        log.info("Down socket verticle deployed with ID: $downSocketVerticleDeploymentId")

        val redisPubSubWorkerOptions = DeploymentOptions()
            .setWorker(true)
            .setWorkerPoolName("redis-pubsub-pool")
            .setWorkerPoolSize(2)
            .setInstances(2)
            .setMaxWorkerExecuteTime(Long.MAX_VALUE)

        redisPubSubWorkerDeploymentId = vertx.deployVerticle(
            "guice:" + RedisPubSubWorkerVerticle::class.java.name,
            redisPubSubWorkerOptions
        ).await()
        log.info("Redis pub/sub worker deployed with ID: $redisPubSubWorkerDeploymentId")

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

        val messageQueueOptions = DeploymentOptions()
            .setWorker(true)
            .setWorkerPoolName("message-queue-consumer-pool")
            .setWorkerPoolSize(1)
            .setInstances(1)

        messageQueueConsumerVerticleDeploymentId = vertx.deployVerticle(
            "guice:" + MessageQueueConsumerVerticle::class.java.name,
            messageQueueOptions
        ).await()
        log.info("KafkaMessageQueueConsumer verticle deployed with ID: $messageQueueConsumerVerticleDeploymentId")

        // Deploy the frame worker verticle
        val frameWorkerOptions = DeploymentOptions()
            .setInstances(1)
            .setConfig(JsonObject().put("workerId", System.getenv("HOSTNAME")))

        val frameWorkerDeploymentId = vertx.deployVerticle(
            "guice:" + FrameWorkerVerticle::class.java.name,
            frameWorkerOptions
        ).await()

        log.info("Frame worker verticle deployed with ID: {}", frameWorkerDeploymentId)

        val initResult = vertx.eventBus().request<JsonObject>(
            UpSocketVerticle.ADDRESS_UP_SOCKET_INITIALIZE,
            JsonObject()
        ).await()

        if (initResult.body().getBoolean("success", false)) {
            log.info("UpSocketVerticle router initialized: {}",
                initResult.body().getString("message"))
        } else {
            log.error("Failed to initialize UpSocketVerticle router")
        }

        registerConnectionEventHandlers()

        setupApplicationRoutes()

        onApplicationStart()

        vertx.eventBus().publish(ADDRESS_WORKER_STARTED, JsonObject()
            .put("workerId", System.getenv("HOSTNAME")))

        log.info("Worker instance deployed with ID: $deploymentID")
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

            if (upSocketVerticleDeploymentId != null) {
                try {
                    vertx.undeploy(upSocketVerticleDeploymentId).await()
                    log.info("Up socket verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying up socket verticle", e)
                }
            }

            if (downSocketVerticleDeploymentId != null) {
                try {
                    vertx.undeploy(downSocketVerticleDeploymentId).await()
                    log.info("Down socket verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying down socket verticle", e)
                }
            }

            if (redisPubSubWorkerDeploymentId != null) {
                try {
                    vertx.undeploy(redisPubSubWorkerDeploymentId).await()
                    log.info("PubSub socket verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying PubSub socket verticle", e)
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

            if (cleanupVerticleDeploymentId != null) {
                try {
                    vertx.undeploy(cleanupVerticleDeploymentId).await()
                    log.info("Cleanup verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying cleanup verticle", e)
                }
            }

            if (messageQueueConsumerVerticleDeploymentId != null) {
                try {
                    vertx.undeploy(messageQueueConsumerVerticleDeploymentId).await()
                    log.info("Message queue consumer verticle undeployed successfully")
                } catch (e: Exception) {
                    log.error("Error undeploying message queue verticle", e)
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
        vertx.eventBus().consumer<JsonObject>(ConnectionEvents.ADDRESS_UP_SOCKET_CONNECTED) { message ->
            val connectionId = message.body().getString("connectionId")
            val traceId = message.body().getString("traceId")

            Companion.log.info("MinareApplication acknowledges up socket for ${connectionId}")

            if (connectionId != null) {
                CoroutineScope(vertx.dispatcher()).launch {
                    handleUpSocketConnected(connectionId, traceId)
                }
            }
        }


        vertx.eventBus().consumer<JsonObject>(ConnectionEvents.ADDRESS_DOWN_SOCKET_CONNECTED) { message ->
            val connectionId = message.body().getString("connectionId")
            val traceId = message.body().getString("traceId")

            Companion.log.info("MinareApplication acknowledges down socket for ${connectionId}")

            if (connectionId != null) {
                CoroutineScope(vertx.dispatcher()).launch {
                    handleDownSocketConnected(connectionId, traceId)
                }
            }
        }

        Companion.log.info("Connection event handlers registered")
    }

    private suspend fun handleUpSocketConnected(connectionId: String, traceId: String?) {
        val state = pendingConnections.computeIfAbsent(connectionId) { ConnectionState() }
        state.upSocketConnected = true
        if (traceId != null) state.traceId = traceId

        checkConnectionComplete(connectionId)
    }

    private suspend fun handleDownSocketConnected(connectionId: String, traceId: String?) {
        val state = pendingConnections.computeIfAbsent(connectionId) { ConnectionState() }
        state.downSocketConnected = true
        if (traceId != null) state.traceId = traceId

        checkConnectionComplete(connectionId)
    }

    private suspend fun checkConnectionComplete(connectionId: String) {
        val state = pendingConnections[connectionId] ?: return

        if (state.upSocketConnected && state.downSocketConnected) {
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
         * Start the application with the given implementation class.
         * This is the main entry point that applications should use.
         */
        fun start(applicationClass: Class<out MinareApplication>, args: Array<String>) {
            val vertxOptions = VertxOptions()

            log.info("Clustering is enabled")

            val clusterManager = HazelcastClusterManager()
            vertxOptions.clusterManager = clusterManager

            Vertx.clusteredVertx(vertxOptions).onComplete { ar ->
                if (ar.succeeded()) {
                    val vertx = ar.result()
                    log.info("Successfully created clustered Vertx instance")
                    completeStartup(vertx, applicationClass, args)
                } else {
                    log.error("Failed to create clustered Vertx instance", ar.cause())
                    exitProcess(1)
                }
            }
        }

        /**
         * Complete the startup process once Vertx is initialized
         */
        private fun completeStartup(
            vertx: Vertx,
            applicationClass: Class<out MinareApplication>,
            args: Array<String>
        ) {
            try {
                val appModule = getApplicationModule(applicationClass)
                log.info("Loaded application module: ${appModule.javaClass.name}")

                val dbName = getDatabaseNameFromModule(appModule)
                log.info("Using database name: $dbName")

                val frameworkModule = MinareModule()
                val upSocketVerticleModule = UpSocketVerticleModule()
                val downSocketVerticleModule = DownSocketVerticleModule()
                val frameCoordinatorVerticleModule = FrameCoordinatorVerticleModule()

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
                            .toInstance(true)
                    }
                }

                val combinedModule = object : AbstractModule() {
                    override fun configure() {
                        // Correct order is required:
                        // framework (provides defaults)
                        install(frameworkModule)
                        install(upSocketVerticleModule)
                        install(downSocketVerticleModule)
                        install(frameCoordinatorVerticleModule)
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