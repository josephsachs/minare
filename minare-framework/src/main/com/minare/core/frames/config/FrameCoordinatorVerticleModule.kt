package com.minare.worker.coordinator.config

import com.google.inject.PrivateModule
import com.google.inject.Provides
import com.google.inject.Singleton
import com.minare.core.config.MinareModule
import com.minare.core.frames.coordinator.FrameCoordinatorVerticle
import com.minare.core.frames.coordinator.services.StartupService
import com.minare.core.utils.vertx.EventBusUtils
import com.minare.core.frames.coordinator.CoordinatorAdminVerticle
import com.minare.core.frames.events.WorkerStateSnapshotCompleteEvent
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
        bind(WorkerStateSnapshotCompleteEvent::class.java).`in`(Singleton::class.java)

        // Request external dependencies that should be provided by parent injector
        requireBinding(Vertx::class.java)
        requireBinding(EventBusUtils::class.java)
        requireBinding(CoroutineScope::class.java)

        expose(FrameCoordinatorVerticle::class.java)
        expose(CoordinatorAdminVerticle::class.java)
        expose(StartupService::class.java)

    }
}