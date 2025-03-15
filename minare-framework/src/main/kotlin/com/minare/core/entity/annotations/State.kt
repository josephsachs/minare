package com.minare.core.entity.annotations

import kotlin.reflect.KClass

/**
 * Annotation to mark fields that should be included in the entity's state
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FIELD)
annotation class State(val className: KClass<*> = Nothing::class)