package com.minare.example.config

import com.google.inject.AbstractModule
import com.minare.config.DatabaseNameProvider
import com.minare.core.entity.EntityFactory
import com.minare.example.ExampleEntityFactory
import javax.inject.Singleton

/**
 * Application-specific Guice module for the Example app.
 * This provides bindings specific to our example application.
 *
 * When combined with the framework through a child injector,
 * bindings defined here will override the framework's default bindings.
 */
class ExampleGuiceModule : AbstractModule(), DatabaseNameProvider {

    override fun configure() {
        // Bind our custom entity factory
        // This will override the framework's default EntityFactory binding
        bind(EntityFactory::class.java).to(ExampleEntityFactory::class.java).`in`(Singleton::class.java)

        // The database name is now provided via the DatabaseNameProvider interface
        // so we don't need to bind it explicitly here
    }

    /**
     * Provide the database name for this application.
     * Implementation of DatabaseNameProvider interface.
     */
    override fun getDatabaseName(): String = "minare_example"
}