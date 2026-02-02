package com.minare.core

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.google.inject.*
import com.google.inject.name.Names
import com.minare.core.MinareApplication.ConnectionEvents.ADDRESS_COORDINATOR_STARTED
import com.minare.core.MinareApplication.ConnectionEvents.ADDRESS_WORKER_STARTED
import com.minare.application.interfaces.AppState
import com.minare.application.adapters.ClusteredAppState
import com.minare.application.config.EntityValidator
import com.minare.application.config.FrameworkConfig
import com.minare.application.config.FrameworkConfigBuilder
import com.minare.core.config.HazelcastInstanceHolder
import com.minare.core.config.*
import com.minare.controller.ConnectionController
import com.minare.core.MinareApplication.ConnectionEvents.ADDRESS_TASK_COORDINATOR_STARTED
import com.minare.worker.upsocket.UpSocketVerticle
import com.minare.core.transport.downsocket.DownSocketVerticle
import com.minare.worker.coordinator.config.FrameCoordinatorVerticleModule
import com.minare.worker.downsocket.config.DownSocketVerticleModule
import com.minare.worker.upsocket.config.UpSocketVerticleModule
import io.vertx.core.DeploymentOptions
import io.vertx.core.Vertx
import io.vertx.core.VertxOptions
import io.vertx.core.http.HttpServer
import io.vertx.core.json.JsonObject
import io.vertx.kotlin.coroutines.CoroutineVerticle
import io.vertx.kotlin.coroutines.await
import io.vertx.kotlin.coroutines.dispatcher
import org.slf4j.LoggerFactory
import java.util.concurrent.ConcurrentHashMap
import javax.inject.Inject
import com.minare.core.config.AppStateProvider
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.factories.MinareVerticleFactory
import com.minare.core.frames.coordinator.CoordinatorAdminVerticle
import com.minare.core.frames.coordinator.CoordinatorTaskVerticle
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle
import com.minare.core.frames.coordinator.services.StartupService
import com.minare.core.frames.services.ActiveWorkerSet
import com.minare.core.frames.services.WorkerRegistryMap
import com.minare.core.frames.worker.FrameWorkerHealthMonitorVerticle
import com.minare.core.frames.worker.FrameWorkerVerticle
import com.minare.core.frames.worker.WorkerOperationHandlerVerticle
import com.minare.core.frames.worker.WorkerTaskVerticle
import com.minare.core.operation.MutationVerticle
import com.minare.core.transport.downsocket.RedisPubSubWorkerVerticle
import com.minare.core.storage.services.StateInitializer
import com.minare.core.transport.CleanupVerticle
import com.minare.exceptions.EntityFactoryException
import com.minare.worker.coordinator.events.WorkerReadinessEvent
import io.vertx.config.ConfigRetriever
import io.vertx.config.ConfigRetrieverOptions
import io.vertx.config.ConfigStoreOptions
import io.vertx.ext.mongo.MongoClient
import kotlinx.coroutines.*
import kotlin.system.exitProcess

/**
 * Base application class that provides the core framework functionality.
 * Applications should extend this class and implement the abstract methods.
 */
abstract class MinareApplication : CoroutineVerticle() {
    private val log = LoggerFactory.getLogger(MinareApplication::class.java)

    // Dependency injections
    @Inject private lateinit var connectionController: ConnectionController
    @Inject private lateinit var startupService: StartupService
    @Inject private lateinit var workerReadinessEvent: WorkerReadinessEvent
    @Inject lateinit var stateInitializer: StateInitializer
    @Inject lateinit var injector: Injector

    // Application utilities
    protected lateinit var appState: AppState

    // Verticle deployment information
    private var processorCount: Number? = null
    private var verticleDeploymentIds = ConcurrentHashMap<String, String>()

    // Connection state
    private val pendingConnections = ConcurrentHashMap<String, ConnectionState>()

    private class ConnectionState {
        var upSocketConnected = false
        var downSocketConnected = false
        var traceId: String? = null
    }

    // Servers
    var httpServer: HttpServer? = null

    object ConnectionEvents {
        const val ADDRESS_UP_SOCKET_CONNECTED = "minare.connection.up.connected"
        const val ADDRESS_DOWN_SOCKET_CONNECTED = "minare.connection.down.connected"
        const val ADDRESS_CONNECTION_COMPLETE = "minare.connection.complete"

        const val ADDRESS_COORDINATOR_STARTED = "minare.cluster.coordinator.started"
        const val ADDRESS_TASK_COORDINATOR_STARTED = "minare.cluster.task.coordinator.started"
        const val ADDRESS_WORKER_STARTED = "minare.cluster.worker.started"
    }

    /**
     * Application hooks
     */

    /**
     * @open
     * Runs before application verticles are launched, in
     * global bootstrapping context.
     */
    protected open suspend fun onApplicationBootStrap() {

    }

    /**
     * @open
     * Override to add application-specific HTTP routes.
     * Runs in worker context.
     */
    protected open suspend fun setupApplicationRoutes() {

    }

    /**
     * @open
     * Called just before the coordinator frame and task verticles
     * have started. Runs in coordinator context.
     */
    protected open suspend fun onCoordinatorStart() {

    }

    /**
     * @open
     * Called just before the coordinator frame and task verticles
     * have started. Runs in coordinator context.
     */
    protected open suspend fun afterCoordinatorStart() {

    }

    /**
     * @open
     * Called just before worker frame and task verticles
     * have started. Runs in all workers' contexts.
     */
    protected open suspend fun onWorkerStart() {

    }

    /**
     * Framework startup
     */

    /**
     * Main startup method. Initializes the dependency injection,
     * database, and starts the server with WebSocket routes.
     */
    override suspend fun start() {
        try {
            val instanceRole = System.getenv("INSTANCE_ROLE") ?:
                throw IllegalStateException("INSTANCE_ROLE environment variable is required")

            processorCount = Runtime.getRuntime().availableProcessors()

            if (instanceRole == "COORDINATOR") stateInitializer.initialize()

            vertx.registerVerticleFactory(MinareVerticleFactory(injector))
            log.info("Registered MinareVerticleFactory")

            // Framework hook
            onApplicationBootStrap()

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
     * Deploy a verticle with the given class and options, and register deployment
     * ID in internal map. Note that map is concurrent and will not contain IDs for
     * verticles deployed to other nodes.
     */
    suspend fun createVerticle(verticleClass: Class<out CoroutineVerticle>, options: DeploymentOptions) {
        // @TODO Use merge reconciliation in Hazelcast to amass another map for all.
        // @TODO Use end of coordinator startup after all-ready is established.
        val id = vertx.deployVerticle("guice:" + verticleClass.name, options).await()

        log.info("MinareApplication: ${verticleClass.name} deployed with ID: {}", id)

        verticleDeploymentIds.put(verticleClass.name, id)
    }

    /**
     * Initializes all verticles belonging to coordinator instance,
     * triggers application hooks at appropriate steps.
     */
    private suspend fun initializeCoordinator() {
        createVerticle(
            CoordinatorAdminVerticle::class.java,
            DeploymentOptions()
                .setInstances(1)
                .setConfig(JsonObject().put("role", "coordinator-admin")
                )
        )

        startupService.checkInitialWorkerStatus()
        startupService.awaitAllWorkersReady(workerReadinessEvent)

        createVerticle(
            FrameWorkerHealthMonitorVerticle::class.java,
            DeploymentOptions()
                .setInstances(1)
                .setConfig(JsonObject().put("role", "coordinator-admin")
                )
        )

        val sharedMap = vertx.sharedData()
            .getClusterWideMap<String, String>("app-state").await()
        appState = ClusteredAppState(sharedMap)

        AppStateProvider.setInstance(appState)

        log.info("Initialized clustered app state for coordinator")

        log.info("Starting application...")
        // Framework hook
        onCoordinatorStart()

        createVerticle(
            FrameCoordinatorVerticle::class.java,
            DeploymentOptions()
                .setInstances(1)
                .setConfig(JsonObject().put("role", "coordinator")
                )
        )

        vertx.eventBus().publish(ADDRESS_COORDINATOR_STARTED, JsonObject())

        createVerticle(
            CoordinatorTaskVerticle::class.java,
            DeploymentOptions()
                .setInstances(1)
                .setConfig(JsonObject().put("role", "coordinator")
                )
        )

        vertx.eventBus().publish(ADDRESS_TASK_COORDINATOR_STARTED, JsonObject())

        afterCoordinatorStart()

        log.info("Application startup completed.")
    }

    /**
     * Initializes all verticles belonging to worker instance,
     * triggers application hooks at appropriate steps.
     */
    private suspend fun initializeWorker() {
        // Get worker ID from hostname or config
        val workerId = System.getenv("HOSTNAME") ?: throw IllegalStateException("Worker ID not configured")

        createVerticle(
            UpSocketVerticle::class.java,
            DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("up-socket-pool")
                .setWorkerPoolSize(5)
                .setInstances(3)
                .setConfig(JsonObject().put("useOwnHttpServer", true)
                )
        )

        createVerticle(
            DownSocketVerticle::class.java,
            DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("down-socket-pool")
                .setWorkerPoolSize(5)
                .setInstances(3)
        )

        createVerticle(
            RedisPubSubWorkerVerticle::class.java,
            DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("redis-pubsub-pool")
                .setWorkerPoolSize(2)
                .setInstances(2)
                .setMaxWorkerExecuteTime(Long.MAX_VALUE)
        )

        createVerticle(
            MutationVerticle::class.java,
            DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("mutation-pool")
                .setWorkerPoolSize(2)
                .setInstances(1)
        )

        createVerticle(
            CleanupVerticle::class.java,
            DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("cleanup-pool")
                .setWorkerPoolSize(1)
                .setInstances(1)
        )

        createVerticle(
            WorkerOperationHandlerVerticle::class.java,
            DeploymentOptions()
                .setWorker(true)
                .setWorkerPoolName("worker-operation-handler-pool")
                .setWorkerPoolSize(1)
                .setInstances(1)
        )

        createVerticle(
            FrameWorkerVerticle::class.java,
            DeploymentOptions()
                .setInstances(1)
                .setConfig(JsonObject().put("workerId", workerId))
        )

        createVerticle(
            WorkerTaskVerticle::class.java,
            DeploymentOptions()
                .setInstances(1)
                .setConfig(JsonObject().put("workerId", workerId))
        )

        val sharedMap = vertx.sharedData()
            .getClusterWideMap<String, String>("app-state").await()
        appState = ClusteredAppState(sharedMap)

        AppStateProvider.setInstance(appState)

        log.info("Initialized clustered app state for worker")

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

        // Delegate application routes setup
        log.info("Setting up application routes...")

        // Application hooks
        try {
            onWorkerStart()
        } finally {
            setupApplicationRoutes()
        }

        registerConnectionEventHandlers()
        workerGetRegistry()

        // Announce worker is ready
        vertx.eventBus().publish(ADDRESS_WORKER_STARTED, JsonObject()
            .put("workerId", workerId))
        log.info("Published worker started event for {}", workerId)

        log.info("Worker instance deployed with ID: $deploymentID")
    }

    /**
     * Get the worker registry. Create if not exists.
     */
    private fun workerGetRegistry() {
        val workerId = System.getenv("HOSTNAME") ?: throw IllegalStateException("HOSTNAME not set")

        // Get the worker registry map from dependency injection
        val workerRegistryMap = injector.getInstance(WorkerRegistryMap::class.java)
        val activeWorkerSet = injector.getInstance(ActiveWorkerSet::class.java)

        // Check if worker was pre-registered by infrastructure
        val existingState = workerRegistryMap.get(workerId)

        if (existingState != null) {
            // Update existing entry (likely PENDING → ACTIVE)
            log.info("Updating pre-registered worker {} to ACTIVE status", workerId)
            existingState.put("status", "ACTIVE")
            existingState.put("lastHeartbeat", System.currentTimeMillis())
            workerRegistryMap.put(workerId, existingState)
            activeWorkerSet.put(workerId)
        } else {
            // Self-register if not pre-registered
            log.info("Self-registering worker {} in distributed registry", workerId)
            val newState = JsonObject()
                .put("workerId", workerId)
                .put("status", "ACTIVE")
                .put("lastHeartbeat", System.currentTimeMillis())
                .put("addedAt", System.currentTimeMillis())
            workerRegistryMap.put(workerId, newState)
            activeWorkerSet.put(workerId)
        }

        log.info("Worker {} registered in distributed map", workerId)
    }

    /**
     * Vert.x lifecycle stop
     */
    override suspend fun stop() {
        val instanceRole = System.getenv("INSTANCE_ROLE") ?:
        throw IllegalStateException("INSTANCE_ROLE environment variable is required")

        log.info("Undeploying ${instanceRole} with ID ${deploymentID}")

        try {
            when (instanceRole) {
                "COORDINATOR" -> {
                    undeployVerticles()
                }
                "WORKER" -> {
                    undeployVerticles()
                }
            }

            log.info("Application stopped gracefully")
        } catch (e: Exception) {
            log.error("Error during application shutdown", e)
            throw e
        }
    }

    /**
     * Undeploy all instance verticles
     */
    private suspend fun undeployVerticles() {
        coroutineScope {
            verticleDeploymentIds.map { item ->
                async {
                    try {
                        vertx.undeploy(item.key)
                        log.info("MinareApplication: ${item.value} ${item.key} undeployed successfully")
                    } catch (e: Exception) {
                        log.error("MinareApplication: Error undeploying ${item.value} ${item.key}", e)
                    }
                }
            }.awaitAll()
        }

        if (httpServer != null) {
            try {
                httpServer?.close()?.await()
                log.info("HTTP server closed successfully")
            } catch (e: Exception) {
                log.error("Error closing HTTP server", e)
            }
        }
    }

    /**
     * Framework Events
     */

    /**
     * Register connection event handlers. Originates in worker context.
     */
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

    /**
     * Route up socket command. Originates in worker context.
     */
    private suspend fun handleUpSocketConnected(connectionId: String, traceId: String?) {
        val state = pendingConnections.computeIfAbsent(connectionId) { ConnectionState() }
        state.upSocketConnected = true
        if (traceId != null) state.traceId = traceId

        checkConnectionComplete(connectionId)
    }

    /**
     * Route down socket command. Originates in worker context.
     */
    private suspend fun handleDownSocketConnected(connectionId: String, traceId: String?) {
        val state = pendingConnections.computeIfAbsent(connectionId) { ConnectionState() }
        state.downSocketConnected = true
        if (traceId != null) state.traceId = traceId

        checkConnectionComplete(connectionId)
    }

    /**
     * Confirm given connection is complete and persist it.
     * Originates in worker context.
     */
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
         * Application bootstrapping and dependency injection.
         */

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
         * Configure Vert.x's Jackson ObjectMapper to properly handle Kotlin types.
         * Must be called before any JSON serialization/deserialization occurs.
         */
        private fun configureJackson() {
            val mapper = io.vertx.core.json.jackson.DatabindCodec.mapper()

            // Register Kotlin module for automatic data class handling
            mapper.registerModule(com.fasterxml.jackson.module.kotlin.KotlinModule.Builder().build())

            log.info("Configured Jackson with KotlinModule for automatic Kotlin data class support")
        }

        /**
         * Test if a Mongo connection is possible to establish with the given configuration
         */
        private suspend fun validateMongo(vertx: Vertx, config: FrameworkConfig): Boolean {
            val mongoUri =  "mongodb://${config.mongo.host}:${config.mongo.port}/${config.mongo.database}?replicaSet=rs0"
            val dbName = config.mongo.database

            val client = MongoClient.create(vertx, JsonObject()
                .put("connection_string", mongoUri)
                .put("db_name", dbName)
                .put("useObjectId", true)
                .put("writeConcern", "majority")
                .put("serverSelectionTimeoutMS", 5000)
                .put("connectTimeoutMS", 10000)
                .put("socketTimeoutMS", 60000))

            return try {
                client.runCommand("ping", JsonObject().put("ping", 1)).await()
                client.close()
                log.info("MongoDB is configured and enabled at $mongoUri")
                true
            } catch (e: Exception) {
                log.warn("MongoDB configured but unreachable: ${e.message}")
                client.close()
                false
            }
        }

        /**
         * Starts clustered Vert.x, reads the framework configuration and builds the dependency injection tree.
         * This is the public interface used by the application to pass its own extension of MinareApplication.
         * Typically we expect this to occur in the Main module of the application.
         */
        fun start(applicationClass: Class<out MinareApplication>, args: Array<String>) {
            val frameworkConfig = getFrameworkConfiguration()

            val vertxOptions = VertxOptions()
            val clusterManager = HazelcastConfigFactory.createConfiguredClusterManager(frameworkConfig.hazelcast.clusterName)

            vertxOptions.clusterManager = clusterManager
            HazelcastInstanceHolder.setClusterManager(clusterManager)

            Vertx.clusteredVertx(vertxOptions).onComplete { ar ->
                if (ar.succeeded()) {
                    val vertx = ar.result()
                    log.info("Clustered Vertx instance created at ${System.currentTimeMillis()}")

                    CoroutineScope(vertx.dispatcher()).launch {
                        completeStartup(vertx, applicationClass, frameworkConfig, args)
                    }
                } else {
                    log.error("Failed to create clustered Vertx instance", ar.cause())
                    exitProcess(1)
                }
            }
        }

        /**
         *
         */
        private fun getFrameworkConfiguration(): FrameworkConfig {
            val env = System.getenv("ENVIRONMENT") ?: "default"
            val configPath = "config/${env}.yml"
            val frameworkConfigBuilder = FrameworkConfigBuilder()

            val stream = Thread.currentThread().contextClassLoader.getResourceAsStream(configPath)
                ?: throw IllegalStateException("Config file not found: $configPath")

            val yaml = org.yaml.snakeyaml.Yaml()
            val map: Map<String, Any> = yaml.load(stream)

            return frameworkConfigBuilder.build(JsonObject(map))
        }

        /**
         * Complete the startup process once Vertx is initialized
         */
        private suspend fun completeStartup(
            vertx: Vertx,
            applicationClass: Class<out MinareApplication>,
            config: FrameworkConfig,
            args: Array<String>
        ) {
            configureJackson()
            config.mongo.enabled = validateMongo(vertx, config)

            try {
                val configModule = object : AbstractModule() {
                    override fun configure() {
                        bind(FrameworkConfig::class.java).toInstance(config)
                    }
                }

                // Framework module requires the name of the configured entity factory for the provider
                val frameworkModule = MinareModule(config)

                val upSocketVerticleModule = UpSocketVerticleModule()
                val downSocketVerticleModule = DownSocketVerticleModule()
                val frameCoordinatorVerticleModule = FrameCoordinatorVerticleModule()

                val appModule = getApplicationModule(applicationClass)
                log.info("Loaded application module: ${appModule.javaClass.name}")

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
                        install(vertxModule)
                        install(configModule)

                        install(frameworkModule)
                        install(upSocketVerticleModule)
                        install(downSocketVerticleModule)
                        install(frameCoordinatorVerticleModule)
                        // Then app module (overrides framework if needed)
                        install(appModule)
                        // Then vertx and database modules

                    }
                }

                val injector = Guice.createInjector(combinedModule)

                // Validate the entities registered in application-level entity factory
                EntityValidator().validate(
                    injector.getInstance(EntityFactory::class.java)
                )

                // Inject the application's dependency modules before deploying
                val app = injector.getInstance(applicationClass)
                injector.injectMembers(app)

                // Now we're ready
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