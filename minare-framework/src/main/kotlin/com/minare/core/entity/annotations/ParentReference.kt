package com.minare.core.entity.annotations

/**
 * Annotation to mark fields that reference a parent entity
 * This relationship affects version propagation
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FIELD)
annotation class ParentReference(val bubble_version: Boolean = true)