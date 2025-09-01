package com.minare.worker.coordinator.config

import com.google.inject.PrivateModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.minare.core.config.MinareModule
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle
import com.minare.core.frames.coordinator.services.StartupService
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.frames.coordinator.CoordinatorAdminVerticle
import com.minare.worker.coordinator.events.*
import io.vertx.core.Vertx
import io.vertx.core.impl.logging.LoggerFactory
import kotlinx.coroutines.CoroutineScope
import kotlin.coroutines.CoroutineContext

/**
 * Specialized Guice module for CoordinatorVerticle and its dependencies.
 * This module provides all the necessary components within the CoordinatorVerticle's scope.
 */
class FrameCoordinatorVerticleModule : PrivateModule() {
    private val log = LoggerFactory.getLogger(MinareModule::class.java)

    override fun configure() {
        bind(FrameCoordinatorVerticle::class.java)
        bind(CoordinatorAdminVerticle::class.java)

        // Coordinator services
        bind(StartupService::class.java).`in`(Singleton::class.java)

        // Event handlers
        bind(InfraAddWorkerEvent::class.java).`in`(Singleton::class.java)
        bind(InfraRemoveWorkerEvent::class.java).`in`(Singleton::class.java)
        bind(WorkerFrameCompleteEvent::class.java).`in`(Singleton::class.java)
        bind(WorkerHeartbeatEvent::class.java).`in`(Singleton::class.java)
        bind(WorkerHealthChangeEvent::class.java).`in`(Singleton::class.java)
        bind(WorkerRegisterEvent::class.java).`in`(Singleton::class.java)
        bind(WorkerReadinessEvent::class.java).`in`(Singleton::class.java)

        // Request external dependencies that should be provided by parent injector
        requireBinding(Vertx::class.java)

        // Expose UpSocketVerticle to the parent injector
        expose(FrameCoordinatorVerticle::class.java)
        expose(CoordinatorAdminVerticle::class.java)
    }

    /**
     * Provides a CoroutineScope using the Vertx dispatcher
     */
    @Provides
    @Singleton
    fun provideCoroutineScope(coroutineContext: CoroutineContext): CoroutineScope {
        return CoroutineScope(coroutineContext)
    }

    /**
     * Provides EventBusUtils for FrameCoordinatorVerticle
     */
    @Provides
    @Singleton
    fun provideEventBusUtils(vertx: Vertx, coroutineContext: CoroutineContext): EventBusUtils {
        return EventBusUtils(vertx, coroutineContext, "FrameCoordinatorVerticle")
    }
}