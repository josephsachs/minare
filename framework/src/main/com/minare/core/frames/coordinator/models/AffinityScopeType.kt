package com.minare.core.frames.coordinator.models

/**
 * Operation() = {
 *     id = autoassign-rruf39-djj9384
 *     entityId = // what we mean by "subject"
 *     entityType = blahblah { ... entity stuff }
 *     delta = whatever we're changing on Subject
 * }
 *
 * By means of scope affinities, the frame coordinator decides which operations go to the same worker, on a per
 * frame basis, using the enemy schema and collections.
 *
 * This is tunable parallelism basically. To begin with we will have only central, config-driven setting.
 *
 * The coordinator achieves this by using internal maps when it is building its frame, which can be wiped out each
 * frame just before manifest preparation. Each time it identifies a scope affinity. Most basic form:
 * has SUBJECT, entity exists and isn't mapped, route entity via hash ring. More complex: PersonB is a recentTarget
 * of PersonA so we are inferring that they're in contention and want them strongly ordered; PersonB came up first in the
 * frame buffer so evaluated first and their entityId claimed the bucket, for now, so PersonA is routed to PersonB's
 * worker. This doesn't matter much, what matters is the aggregate effect of the developer strongly entangling their
 * schema, which will create scalability problems for them later; this means that even a composable global affinity
 * configuration is ultimately a blunt instrument. However, once this is tested we will explore overriding affinity
 * settings when composing OperationSets or via Entity annotations, which will allow more granular composition
 * of concerns. (This is why we are using a Set right now, even though it might not seem necessary.)
 *
 *
 */

enum class AffinityScopeType {
    SUBJECT,        // Top-level subjects, self-grouped
    OPERATION_SET,  // Subjects of operation sets all route with the first
    FIELD_PARENT,   // Entities are lumped in by parent field relationships
    FIELD_PEER,     // or by peer field relationships
    PEER_CHILD,     // or by children's fields are lumped in
}