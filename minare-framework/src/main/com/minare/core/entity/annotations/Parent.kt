package com.minare.core.entity.annotations

/**
 * Entity field is a key to a child entity
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FIELD)
annotation class Parent(val bubble_version: Boolean = true)