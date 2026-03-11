package com.minare.core.frames.coordinator.models

enum class AffinityScopeType {
    SUBJECT,        // Top-level subjects, self-grouped
    OPERATION_SET,  // Subjects of operation sets all route with the first
    FIELD_PARENT,   // Entities are lumped in by parent field relationships
    FIELD_PEER,     // or by peer field relationships
    PEER_CHILD,     // or by children's fields are lumped in
}