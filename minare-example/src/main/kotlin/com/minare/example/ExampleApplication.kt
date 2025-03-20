package com.minare.example

import com.minare.MinareApplication
import com.minare.example.config.ExampleGuiceModule
import com.minare.example.controller.ChannelController
import com.minare.example.core.models.Node
import com.minare.example.core.models.NodeGraphBuilder
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
    private lateinit var defaultChannelId: String

    @Inject
    lateinit var channelController: ChannelController

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
            defaultChannelId = channelController.createChannel()
            log.info("Created default channel: $defaultChannelId")

            // Initialize the node graph
            initializeNodeGraph(defaultChannelId)

            log.info("Example application started with default channel: $defaultChannelId")
        } catch (e: Exception) {
            log.error("Failed to start example application", e)
            throw e
        }
    }

    // This is the single implementation of the route setup method
    override fun setupApplicationRoutes(router: Router) {
        // Serve static content from the resources/example.webroot directory
        val staticHandler = StaticHandler.create()
            .setWebRoot("example.webroot")  // Path relative to resources directory
            .setCachingEnabled(false)       // Disable caching for development
            .setDirectoryListing(true)      // Optional: Enable directory listing for debugging
            .setIncludeHidden(false)        // Don't serve hidden files
            .setFilesReadOnly(true)        // Read-only access to files

        router.route("/*").handler(staticHandler)

        log.info("Example application routes configured")
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