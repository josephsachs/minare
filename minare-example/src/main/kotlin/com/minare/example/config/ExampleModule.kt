package com.minare.example.config

import com.google.inject.PrivateModule
import com.google.inject.Singleton
import com.minare.config.DatabaseNameProvider
import com.minare.controller.ConnectionController
import com.minare.core.entity.EntityFactory
import com.minare.example.ExampleEntityFactory
import com.minare.example.controller.ChannelController
import com.minare.example.controller.ExampleChannelController
import com.minare.example.controller.ExampleConnectionController
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


        bind(EntityFactory::class.java).to(ExampleEntityFactory::class.java).`in`(Singleton::class.java)


        bind(ChannelController::class.java).to(ExampleChannelController::class.java).`in`(Singleton::class.java)
        bind(ConnectionController::class.java).to(ExampleConnectionController::class.java).`in`(Singleton::class.java)


        expose(EntityFactory::class.java)
        expose(ChannelController::class.java)
        expose(ConnectionController::class.java)

        log.info("ExampleModule configured with custom EntityFactory and controllers")
    }

    override fun getDatabaseName(): String = "minare_example"
}