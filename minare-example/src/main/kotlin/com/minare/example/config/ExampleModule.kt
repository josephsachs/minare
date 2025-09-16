package com.minare.example.config

import com.google.inject.PrivateModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.google.inject.name.Names
import com.minare.controller.ChannelController
import com.minare.controller.ConnectionController
import com.minare.controller.MessageController
import com.minare.controller.OperationController
import com.minare.core.config.DatabaseNameProvider
import com.minare.core.entity.factories.EntityFactory
import com.minare.example.ExampleEntityFactory
import com.minare.example.controller.ExampleChannelController
import com.minare.example.controller.ExampleConnectionController
import com.minare.example.controller.ExampleMessageController
import com.minare.example.controller.ExampleOperationController
import kotlinx.coroutines.CoroutineScope
import org.slf4j.LoggerFactory
import kotlin.coroutines.CoroutineContext

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
        bind(EntityFactory::class.java).annotatedWith(Names.named("userEntityFactory"))
            .to(ExampleEntityFactory::class.java).`in`(Singleton::class.java)

        bind(ChannelController::class.java).to(ExampleChannelController::class.java).`in`(Singleton::class.java)
        bind(ConnectionController::class.java).to(ExampleConnectionController::class.java).`in`(Singleton::class.java)
        bind(OperationController::class.java).to(ExampleOperationController::class.java).`in`(Singleton::class.java)
        bind(MessageController::class.java).to(ExampleMessageController::class.java).`in`(Singleton::class.java)

        // Expose the named user factory (framework will wrap it)
        expose(EntityFactory::class.java).annotatedWith(Names.named("userEntityFactory"))
        expose(ChannelController::class.java)
        expose(ConnectionController::class.java)
        expose(OperationController::class.java)
        expose(MessageController::class.java)

        log.info("ExampleModule configured with custom EntityFactory and controllers")
    }

    @Provides
    @Singleton
    fun provideCoroutineScope(coroutineContext: CoroutineContext): CoroutineScope {
        return CoroutineScope(coroutineContext)
    }

    override fun getDatabaseName(): String = "minare_example"
}