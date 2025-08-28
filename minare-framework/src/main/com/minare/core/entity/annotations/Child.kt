package com.minare.core.entity.annotations

/**
 * Annotation to mark fields that reference child entities
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FIELD)
annotation class Child