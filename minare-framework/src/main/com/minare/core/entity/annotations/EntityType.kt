package com.minare.core.entity.annotations

/**
 * Defines an Entity type for active state management
 */
@Retention(AnnotationRetention.RUNTIME)
annotation class EntityType(val value: String) { }