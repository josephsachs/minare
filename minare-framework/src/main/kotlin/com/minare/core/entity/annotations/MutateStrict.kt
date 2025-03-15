package com.minare.core.entity.annotations

/**
 * Annotation to indicate fields that require strict mutation rules
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FIELD)
annotation class MutateStrict(val name: String = "")