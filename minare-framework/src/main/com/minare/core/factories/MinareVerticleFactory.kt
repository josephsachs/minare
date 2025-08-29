package com.minare.core.factories

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
    override fun prefix(): String = "guice"

    /**
     * Creates a verticle instance using Guice dependency injection.
     * Important: We use the injector to create a fresh instance even for classes
     * that might be bound as singletons.
     */
    override fun createVerticle(verticleName: String, classLoader: ClassLoader, promise: Promise<Callable<Verticle>>) {
        try {
            val className = VerticleFactory.removePrefix(verticleName)
            log.debug("Creating verticle instance for: {}", className)
            val verticleClass = classLoader.loadClass(className) as Class<out Verticle>

            val verticleCallable = Callable<Verticle> {
                // Provider.get() will create a new instance even for singleton-bound classes
                val verticle = injector.getInstance(verticleClass)
                log.debug("Instantiated verticle: {}", verticle::class.java.name)
                verticle
            }

            promise.complete(verticleCallable)
        } catch (e: Exception) {
            log.error("Failed to create verticle: {}", verticleName, e)
            promise.fail(e)
        }
    }
}