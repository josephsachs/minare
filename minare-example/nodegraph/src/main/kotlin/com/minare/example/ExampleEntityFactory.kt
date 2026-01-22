package com.minare.example

import com.google.inject.Inject
import com.minare.core.entity.factories.EntityFactory
import com.minare.example.models.Node
import javax.inject.Singleton

/**
 * Example EntityFactory implementation.
 * Updated to remove dependency injection since Entity is now a pure data class.
 */
@Singleton
class ExampleEntityFactory @Inject constructor() : EntityFactory() {

    // Just define the map - framework does the rest!
    override val entityTypes = mapOf(
        "Node" to Node::class.java
    )
}