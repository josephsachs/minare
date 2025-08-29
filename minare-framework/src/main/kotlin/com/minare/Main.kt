package com.minare

import io.vertx.core.Vertx
import org.slf4j.LoggerFactory

/**
 * Main utility class for the Minare framework.
 * Provides helper methods for starting applications.
 */
class Main {
    companion object {
        private val log = LoggerFactory.getLogger(Main::class.java)

        /**
         * A utility method to deploy a MinareApplication instance.
         * This can be used by implementing applications as a convenience.
         */
        @JvmStatic
        fun deployApplication(applicationClass: Class<out MinareApplication>) {
            val vertx = Vertx.vertx()

            try {
                log.info("Deploying Minare application: ${applicationClass.simpleName}")


                vertx.deployVerticle(applicationClass.getDeclaredConstructor().newInstance())
                    .onSuccess { id ->
                        log.info("Successfully deployed application with ID: {}", id)
                    }
                    .onFailure { err ->
                        log.error("Failed to deploy application", err)
                        vertx.close()
                        System.exit(1)
                    }


                Runtime.getRuntime().addShutdownHook(Thread {
                    log.info("Shutting down Minare application")
                    vertx.close()
                })

            } catch (e: Exception) {
                log.error("Failed to initialize application", e)
                vertx.close()
                System.exit(1)
            }
        }
    }
}