package com.minare.example

import com.minare.MinareApplication
import com.minare.example.config.ExampleGuiceModule
import com.minare.example.controller.ExampleChannelController
import com.minare.example.core.models.NodeGraphBuilder
import io.vertx.core.buffer.Buffer
import io.vertx.core.http.HttpHeaders
import io.vertx.ext.web.Router
import io.vertx.ext.web.handler.StaticHandler
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
    lateinit var entityFactory: ExampleEntityFactory

    @Inject
    lateinit var nodeGraphBuilder: NodeGraphBuilder

    /**
     * Application-specific initialization logic that runs after the server starts.
     */
    override suspend fun onApplicationStart() {
        try {
            // Create a default channel for testing
            val defaultChannelId = channelController.createChannel()
            log.info("Created default channel: $defaultChannelId")

            // Set this as the default channel in the controller
            channelController.setDefaultChannel(defaultChannelId)

            // Initialize the node graph
            initializeNodeGraph(defaultChannelId)

            log.info("Example application started with default channel: $defaultChannelId")
        } catch (e: Exception) {
            log.error("Failed to start example application", e)
            throw e
        }
    }

    // Rest of the class remains unchanged

    // This is the single implementation of the route setup method
    override fun setupApplicationRoutes(router: Router) {
        val staticHandler = StaticHandler.create()
            .setCachingEnabled(false)  // Disable caching during development
            .setAllowRootFileSystemAccess(false)
            .setDefaultContentEncoding("UTF-8")
            .setFilesReadOnly(true)

        router.route("/*").handler(staticHandler)

        router.get("/client").handler { ctx ->
            // Try to read the file directly from resources
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

        // Add a debug endpoint to check classpath
        router.get("/debug-resources").handler { ctx ->
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
    }

    /**
     * Creates a sample node graph with parent/child relationships
     * using JGraphT to generate an asymmetric tree structure
     */
    private suspend fun initializeNodeGraph(channelId: String) {
        try {
            // Build an asymmetric tree using our graph builder
            // Now returns both the root node and a list of all nodes
            val (rootNode, allNodes) = nodeGraphBuilder.buildAsymmetricTree(
                maxDepth = 4,           // Up to 4 levels deep
                maxBranchingFactor = 3, // At most 3 children per node
                totalNodes = 25,        // Aim for about 25 nodes total
                randomSeed = 42L        // Fixed seed for reproducibility
            )

            // Add all nodes to the channel using the channel controller
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
        fun getModule() = ExampleGuiceModule()
    }
}