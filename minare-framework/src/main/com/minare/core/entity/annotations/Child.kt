package com.minare.core.entity.annotations

import com.minare.core.entity.models.Entity

/**
 * Annotation to mark fields that reference child entities
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FIELD)
annotation class Child