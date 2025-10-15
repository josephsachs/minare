package com.minare.example

import com.google.inject.Inject
import com.google.inject.Injector
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.models.Entity
import com.minare.example.models.Node
import org.slf4j.LoggerFactory
import javax.inject.Singleton
import kotlin.reflect.KClass

/**
 * Example EntityFactory implementation.
 * Updated to remove dependency injection since Entity is now a pure data class.
 */
@Singleton
class ExampleEntityFactory @Inject constructor(
    injector: Injector
) : EntityFactory(injector) {

    // Just define the map - framework does the rest!
    override val entityTypes = mapOf(
        "Node" to Node::class.java
    )
}