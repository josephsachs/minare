package com.minare.example.config

import com.google.inject.PrivateModule
import com.google.inject.Singleton
import com.google.inject.name.Names
import com.minare.controller.ChannelController
import com.minare.controller.ConnectionController
import com.minare.controller.OperationController
import com.minare.core.config.DatabaseNameProvider
import com.minare.core.entity.EntityFactory
import com.minare.example.ExampleEntityFactory
import com.minare.example.controller.ExampleChannelController
import com.minare.example.controller.ExampleConnectionController
import com.minare.example.controller.ExampleOperationController
import org.slf4j.LoggerFactory

/**
 * Application-specific Guice module for the Example app.
 * This provides bindings specific to our example application.
 *
 * When combined with the framework through a child injector,
 * bindings defined here will override the framework's default bindings.
 */
class ExampleModule : PrivateModule(), DatabaseNameProvider {
    private val log = LoggerFactory.getLogger(ExampleModule::class.java)

    override fun configure() {
        // Bind our custom EntityFactory
        bind(EntityFactory::class.java).annotatedWith(Names.named("userEntityFactory"))
            .to(ExampleEntityFactory::class.java).`in`(Singleton::class.java)

        bind(ChannelController::class.java).to(ExampleChannelController::class.java).`in`(Singleton::class.java)
        bind(ConnectionController::class.java).to(ExampleConnectionController::class.java).`in`(Singleton::class.java)
        bind(OperationController::class.java).to(ExampleOperationController::class.java).`in`(Singleton::class.java)

        // Expose the named user factory (framework will wrap it)
        expose(EntityFactory::class.java).annotatedWith(Names.named("userEntityFactory"))
        expose(ChannelController::class.java)
        expose(ConnectionController::class.java)
        expose(OperationController::class.java)

        log.info("ExampleModule configured with custom EntityFactory and controllers")
    }

    override fun getDatabaseName(): String = "minare_example"
}