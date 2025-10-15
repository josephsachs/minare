package com.minare.core.entity.annotations

/**
 * Entity field allowed to be mutated by operation
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FIELD)
annotation class Mutable(
    val consistency: ConsistencyLevel = ConsistencyLevel.OPTIMISTIC
) {
    companion object {
        /**
         * Defines consistency levels for mutable fields
         */
        enum class ConsistencyLevel {
            OPTIMISTIC,  // Allow changes, resolve conflicts later if needed
            PESSIMISTIC, // Verify version before allowing changes
            STRICT       // Most restrictive, require exact version match
        }
    }
}