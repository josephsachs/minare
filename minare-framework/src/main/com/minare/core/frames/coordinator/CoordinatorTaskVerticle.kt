package com.minare.core.frames.coordinator

import com.google.inject.Inject
import com.minare.core.entity.ReflectionCache
import io.vertx.core.impl.logging.LoggerFactory
import io.vertx.kotlin.coroutines.CoroutineVerticle

class CoordinatorTaskVerticle @Inject constructor(
    private val reflectionCache: ReflectionCache
): CoroutineVerticle() {
    private val log = LoggerFactory.getLogger(CoordinatorTaskVerticle::class.java)


}