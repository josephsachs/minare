package com.minare.core.entity

import com.minare.core.models.Entity
import kotlin.reflect.KClass

/**
 * Factory interface for creating entity instances by type.
 */
interface EntityFactory {
    /**
     * Creates a new entity instance of the given type.
     *
     * @param type The entity type
     * @return A new entity instance
     * @throws IllegalArgumentException if the type is not recognized
     */
    fun getNew(type: String): Entity

    /**
     * Registers an entity class with its type.
     *
     * @param type The entity type
     * @return The entity class
     */
    fun useClass(type: String): Class<*>?

    fun getTypeNames(): List<String>

    fun getTypeList(): List<KClass<*>>
}