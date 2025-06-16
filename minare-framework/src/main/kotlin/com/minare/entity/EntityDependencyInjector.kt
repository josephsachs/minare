package com.minare.entity

import com.minare.core.entity.ReflectionCache
import com.minare.core.models.Entity
import com.minare.persistence.StateStore
import javax.inject.Inject
import javax.inject.Singleton

/**
 * Framework service responsible for injecting internal dependencies into entities.
 * This ensures all entities have the required framework services without exposing
 * these dependencies to user code.
 */
@Singleton
class EntityDependencyInjector @Inject constructor(
    private val reflectionCache: ReflectionCache,
    private val stateStore: StateStore,
    private val versioningService: EntityVersioningService
) {
    /**
     * Inject all required framework dependencies into an entity.
     * This is called automatically by the framework after entity creation.
     *
     * @param entity The entity to inject dependencies into
     * @return The same entity with dependencies injected
     */
    fun <T : Entity> injectDependencies(entity: T): T {
        //entity.reflectionCache = reflectionCache
        //entity.stateStore = stateStore
        //entity.versioningService = versioningService
        return entity
    }
}