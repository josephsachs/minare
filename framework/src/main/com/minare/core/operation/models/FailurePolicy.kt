package com.minare.core.operation.models

enum class FailurePolicy {
    /** Proceed regardless of step failure. Completed deltas stand. */
    CONTINUE,

    /** Halt remaining steps on failure. Completed deltas stand. */
    ABORT,

    /** Halt remaining steps on failure. Reverse completed mutations through the normal handler path. */
    ROLLBACK
}
