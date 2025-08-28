package com.minare.example

import com.minare.core.MinareApplication
import com.minare.example.config.ExampleModule
import com.minare.example.controller.ExampleChannelController
import com.minare.example.core.models.NodeGraphBuilder
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.BodyHandler
import io.vertx.ext.web.handler.StaticHandler
import io.vertx.kotlin.coroutines.await
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * Example application that demonstrates the framework capabilities.
 * Creates a complex node graph with parent/child relationships using JGraphT.
 */
class ExampleApplication : MinareApplication() {
    private val log = LoggerFactory.getLogger(ExampleApplication::class.java)

    @Inject
    lateinit var channelController: ExampleChannelController

    @Inject
    lateinit var nodeGraphBuilder: NodeGraphBuilder

    /**
     * Application-specific initialization logic that runs after the server starts.
     */
    override suspend fun onApplicationStart() {
        try {
            val defaultChannelId = channelController.createChannel()
            log.info("Created default channel: $defaultChannelId")

            channelController.setDefaultChannel(defaultChannelId)

            initializeNodeGraph(defaultChannelId)

            log.info("Example application started with default channel: $defaultChannelId")
        } catch (e: Exception) {
            log.error("Failed to start example application", e)
            throw e
        }
    }

    override suspend fun setupApplicationRoutes() {
        val router = Router.router(vertx)
        router.route().handler(BodyHandler.create())

        val serverPort = 8080
        httpServer = vertx.createHttpServer()
            .requestHandler(router)
            .listen(serverPort)
            .await()

        log.info("Main application HTTP server started on port {}", serverPort)

        // Register specific routes FIRST
        router.get("/client").handler { ctx ->
            val resource = Thread.currentThread().contextClassLoader.getResourceAsStream("webroot/index.html")

            if (resource != null) {
                val content = resource.readAllBytes()
                ctx.response()
                    .putHeader(HttpHeaders.CONTENT_TYPE, "text/html")
                    .end(Buffer.buffer(content))
            } else {
                ctx.response()
                    .setStatusCode(404)
                    .end("Couldn't find index.html in resources")
            }
        }

        router.get("/debug").handler { ctx ->
            val classLoader = Thread.currentThread().contextClassLoader
            val resourceUrl = classLoader.getResource("webroot/index.html")
            val resourceStream = classLoader.getResourceAsStream("webroot/index.html")

            val response = StringBuilder()
            response.append("Resource URL: ${resourceUrl}\n")
            response.append("Resource Stream null? ${resourceStream == null}\n")

            if (resourceStream != null) {
                val content = String(resourceStream.readAllBytes())
                response.append("Content length: ${content.length}\n")
                response.append("First 100 chars: ${content.take(100)}\n")
            }

            ctx.response()
                .putHeader("content-type", "text/plain")
                .end(response.toString())
        }

        // Register catch-all static handler LAST
        val staticHandler = StaticHandler.create()
            .setCachingEnabled(false)
            .setDefaultContentEncoding("UTF-8")
            .setFilesReadOnly(true)

        router.route("/*").handler(staticHandler)
    }

    /**
     * Creates a sample node graph with parent/child relationships
     * using JGraphT to generate an asymmetric tree structure
     */
    private suspend fun initializeNodeGraph(channelId: String) {
        try {
            val (rootNode, allNodes) = nodeGraphBuilder.buildAsymmetricTree(
                maxDepth = 4,
                maxBranchingFactor = 3,
                totalNodes = 25,
                randomSeed = 42L
            )

            val nodesAdded = channelController.addEntitiesToChannel(allNodes, channelId)
            log.info("Added $nodesAdded nodes to channel $channelId")

            log.info("Node graph initialized successfully")
        } catch (e: Exception) {
            log.error("Failed to initialize node graph", e)
            throw e
        }
    }

    companion object {
        /**
         * Returns the Guice module for this application
         */
        @JvmStatic
        fun getModule() = ExampleModule()
    }
}