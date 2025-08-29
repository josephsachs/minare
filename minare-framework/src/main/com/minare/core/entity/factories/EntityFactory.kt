package com.minare.core.entity.factories

import com.minare.core.entity.models.Entity
import kotlin.reflect.KClass

/**
 * Interface for entity factories that create and manage entity classes.
 * Applications can implement this interface to provide their own entity types.
 *
 * Framework dependencies are automatically injected into all created entities
 * by the framework, so implementations should focus solely on entity creation.
 */
interface EntityFactory {
    /**
     * Get the Java class for a given entity type name
     */
    fun useClass(type: String): Class<*>?

    /**
     * Create a new instance of an entity based on its type name.
     * Framework dependencies will be automatically injected.
     */
    fun getNew(type: String): Entity

    /**
     * Create a new instance of an entity based on its class.
     * This version allows for type-safe entity creation.
     * Framework dependencies will be automatically injected.
     */
    fun <T : Entity> createEntity(entityClass: Class<T>): T

    /**
     * Get a list of registered entity type names
     */
    fun getTypeNames(): List<String>

    /**
     * Get a list of registered entity type Kotlin classes
     */
    fun getTypeList(): List<KClass<*>>
}