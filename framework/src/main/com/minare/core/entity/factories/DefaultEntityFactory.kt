package com.minare.core.entity.factories

import com.google.inject.Inject
import com.google.inject.Injector
import com.minare.core.entity.models.Entity
import com.google.inject.Singleton

/**
 * Default implementation that only provides the base Entity type.
 * Applications should override this with their own implementation.
 */
@Singleton
class DefaultEntityFactory @Inject constructor() : EntityFactory() {

    override val entityTypes = mapOf(
        "Entity" to Entity::class.java
    )
}