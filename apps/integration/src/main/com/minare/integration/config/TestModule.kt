package com.minare.integration.config

import com.google.inject.PrivateModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.multibindings.OptionalBinder
import com.google.inject.name.Names
import com.minare.controller.ChannelController
import com.minare.controller.ConnectionController
import com.minare.controller.MessageController
import com.minare.controller.OperationController
import com.minare.core.config.DatabaseNameProvider
import com.minare.core.entity.factories.EntityFactory
import com.minare.integration.TestEntityFactory
import com.minare.integration.controller.TestChannelController
import com.minare.integration.controller.TestConnectionController
import com.minare.integration.controller.TestMessageController
import com.minare.integration.controller.TestOperationController
import org.slf4j.LoggerFactory

/**
 * Application-specific Guice module for the test app.
 * This provides bindings specific to our test application.
 *
 * When combined with the framework through a child injector,
 * bindings defined here will override the framework's default bindings.
 */
class TestModule : PrivateModule(), DatabaseNameProvider {
    private val log = LoggerFactory.getLogger(TestModule::class.java)

    override fun configure() {
        bind(ChannelController::class.java).to(TestChannelController::class.java).`in`(Singleton::class.java)
        bind(ConnectionController::class.java).to(TestConnectionController::class.java).`in`(Singleton::class.java)
        bind(OperationController::class.java).to(TestOperationController::class.java).`in`(Singleton::class.java)
        bind(MessageController::class.java).to(TestMessageController::class.java).`in`(Singleton::class.java)

        expose(ChannelController::class.java)
        expose(ConnectionController::class.java)
        expose(OperationController::class.java)
        expose(MessageController::class.java)

        log.info("TestModule configured with custom EntityFactory and controllers")
    }

    override fun getDatabaseName(): String = "integration"
}