package com.minare.core.entity

import com.minare.core.models.Entity
import kotlin.reflect.KClass

/**
 * Interface for entity factories that create and manage entity classes.
 * Applications can implement this interface to provide their own entity types.
 */
interface EntityFactory {
    /**
     * Get the Java class for a given entity type name
     */
    fun useClass(type: String): Class<*>?

    /**
     * Create a new instance of an entity based on its type name
     */
    fun getNew(type: String): Entity

    /**
     * Create a new instance of an entity based on its class
     * This version allows for type-safe entity creation
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