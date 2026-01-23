package com.minare.core.entity.annotations

/**
 * Entity field allowed to be mutated by operation
 */
@Retention(AnnotationRetention.RUNTIME)
@Target(AnnotationTarget.FIELD)
annotation class Mutable(
    val validationPolicy: ValidationPolicy = ValidationPolicy.NONE
) {
    companion object {
        /**
         * Defines consistency levels for mutable fields
         */
        enum class ValidationPolicy {
            NONE,       // Allow all well-formed changes
            FIELD,      // Omit invalid fields
            ENTITY,     // Reject entire entity delta if rule fails
            OPERATION   // Reject entire operation if rule fails
        }
    }
}