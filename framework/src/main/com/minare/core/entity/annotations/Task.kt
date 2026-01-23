package com.minare.core.entity.annotations

/**
 * Entity member function runs as a Task
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FUNCTION)
annotation class Task { }