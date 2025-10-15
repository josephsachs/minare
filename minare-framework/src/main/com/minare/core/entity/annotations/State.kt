package com.minare.core.entity.annotations

/**
 * Entity field will be included in managed state
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FIELD)
annotation class State(val fieldName: String = "")