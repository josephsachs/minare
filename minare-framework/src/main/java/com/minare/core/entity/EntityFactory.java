package com.minare.core.entity;

import com.minare.core.models.Entity;

/**
 * Factory interface for creating entity instances by type.
 */
public interface EntityFactory {

    /**
     * Creates a new entity instance of the given type.
     *
     * @param type The entity type
     * @return A new entity instance
     * @throws IllegalArgumentException if the type is not recognized
     */
    Entity getNew(String type);

    /**
     * Registers an entity class with its type.
     *
     * @param type The entity type
     * @return Class<?> The entity class
     */
    Class<?> useClass(String type);
}