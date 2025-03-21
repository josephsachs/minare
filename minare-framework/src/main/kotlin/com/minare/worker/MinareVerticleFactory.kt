package com.minare.worker

import com.google.inject.Injector
import io.vertx.core.Promise
import io.vertx.core.Verticle
import io.vertx.core.spi.VerticleFactory
import org.slf4j.LoggerFactory
import java.util.concurrent.Callable

/**
 * A VerticleFactory implementation that uses Guice to instantiate verticles,
 * ensuring all dependencies are properly injected.
 */
public final class MinareVerticleFactory(private val injector: Injector) : VerticleFactory {
    private val log = LoggerFactory.getLogger(MinareVerticleFactory::class.java)

    // Prefix for verticles that will be instantiated by this factory
    override fun prefix(): String = "guice"

    /**
     * Creates a verticle instance using Guice dependency injection
     */
    override fun createVerticle(verticleName: String, classLoader: ClassLoader, promise: Promise<Callable<Verticle>>) {
        try {
            // Remove the prefix from the verticle name
            val className = VerticleFactory.removePrefix(verticleName)
            log.debug("Creating verticle instance for: {}", className)

            // Load the verticle class
            val verticleClass = classLoader.loadClass(className)

            // Create a callable that will return a Guice-managed verticle when called
            val verticleCallable = Callable<Verticle> {
                val verticle = injector.getInstance(verticleClass) as Verticle
                log.debug("Instantiated Guice-managed verticle: {}", verticle::class.java.name)
                verticle
            }

            // Complete the promise with the callable
            promise.complete(verticleCallable)
        } catch (e: Exception) {
            log.error("Failed to create verticle: {}", verticleName, e)
            promise.fail(e)
        }
    }
}