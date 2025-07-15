package com.minare.worker.coordinator.config

import com.google.inject.PrivateModule
import com.minare.config.MinareModule
import com.minare.worker.coordinator.CoordinatorVerticle
import io.vertx.core.Vertx
import io.vertx.core.impl.logging.LoggerFactory

/**
 * Specialized Guice module for CoordinatorVerticle and its dependencies.
 * This module provides all the necessary components within the CoordinatorVerticle's scope.
 */
class CoordinatorVerticleModule : PrivateModule() {
    private val log = LoggerFactory.getLogger(MinareModule::class.java)

    override fun configure() {
        bind(CoordinatorVerticle::class.java)

        requireBinding(Vertx::class.java)
    }
}