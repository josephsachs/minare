package com.minare.core.entity.annotations

/**
 * Annotation to indicate fields that can be mutated and their consistency requirements
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FIELD)
annotation class Mutable(
    val name: String = "",
    val consistency: ConsistencyLevel = ConsistencyLevel.OPTIMISTIC
)

/**
 * Defines consistency levels for mutable fields
 */
enum class ConsistencyLevel {
    OPTIMISTIC,  // Allow changes, resolve conflicts later if needed
    PESSIMISTIC, // Verify version before allowing changes
    STRICT       // Most restrictive, require exact version match
}
