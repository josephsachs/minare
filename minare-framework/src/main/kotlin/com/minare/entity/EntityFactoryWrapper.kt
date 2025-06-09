package com.minare.entity

import com.minare.core.entity.EntityFactory
import com.minare.core.models.Entity
import kotlin.reflect.KClass

/**
 * Framework wrapper around user EntityFactory implementations that automatically
 * injects framework dependencies into all created entities.
 *
 * This maintains the existing EntityFactory API while ensuring all entities
 * receive proper dependency injection without exposing framework internals
 * to user code.
 */
class EntityFactoryWrapper(
    private val delegate: EntityFactory,
    private val dependencyInjector: EntityDependencyInjector
) : EntityFactory {

    // Delegate methods that don't create entities - no injection needed
    override fun useClass(type: String): Class<*>? = delegate.useClass(type)
    override fun getTypeNames(): List<String> = delegate.getTypeNames()
    override fun getTypeList(): List<KClass<*>> = delegate.getTypeList()

    // Entity creation methods - automatically inject dependencies
    override fun getNew(type: String): Entity {
        val entity = delegate.getNew(type)
        return dependencyInjector.injectDependencies(entity)
    }

    override fun <T : Entity> createEntity(entityClass: Class<T>): T {
        val entity = delegate.createEntity(entityClass)
        return dependencyInjector.injectDependencies(entity)
    }
}