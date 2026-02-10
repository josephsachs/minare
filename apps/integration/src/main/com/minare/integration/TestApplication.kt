package com.minare.integration

import com.minare.core.MinareApplication
import com.minare.integration.config.TestModule
import com.minare.integration.controller.TestChannelController
import com.minare.integration.harness.IntegrationTestRunner
import com.minare.integration.suites.SmokeTestSuite
import com.minare.integration.suites.EntityControllerTestSuite
import com.minare.integration.suites.OperationTestSuite
import org.slf4j.LoggerFactory
import javax.inject.Inject

/**
 * Test application that demonstrates the framework capabilities.
 * Creates a complex node graph with parent/child relationships using JGraphT.
 */
class TestApplication : MinareApplication() {
    private val log = LoggerFactory.getLogger(TestApplication::class.java)

    @Inject
    lateinit var channelController: TestChannelController

    /**
     * Application-specific initialization logic that runs after the server starts.
     */
    override suspend fun onCoordinatorStart() {
        try {
            val defaultChannelId = channelController.createChannel()
            log.info("Created default channel: $defaultChannelId")

            channelController.setDefaultChannel(defaultChannelId)

            log.info("Test application started with default channel: $defaultChannelId")
        } catch (e: Exception) {
            log.error("Failed to start test application", e)
            throw e
        }
    }

    override suspend fun afterCoordinatorStart() {
        val testRunner = IntegrationTestRunner()
        testRunner.runAllAndExit(
            SmokeTestSuite(injector),
            EntityControllerTestSuite(injector),
            OperationTestSuite(injector)
        )
    }

    override suspend fun setupApplicationRoutes() {
        // Silence is golden
    }

    companion object {
        /**
         * Returns the Guice module for this application
         */
        @JvmStatic
        fun getModule() = TestModule()
    }
}