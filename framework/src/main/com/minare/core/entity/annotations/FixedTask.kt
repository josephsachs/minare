package com.minare.core.entity.annotations

/**
 * Entity member function runs as a frame-bound Task
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FUNCTION)
annotation class FixedTask { }