package com.minare.core.frames.coordinator.models

/**
 * Controls which operations the frame coordinator routes to the same worker, per frame.
 *
 * This is tunable parallelism. The coordinator builds an internal affinity map during
 * manifest preparation — entityId → workerId — that is wiped each frame. Each scope type
 * adds related entity IDs to the map so they co-locate on the same worker.
 *
 * Most basic form: ENTITY — each operation's entityId claims a worker slot via hash ring.
 * More complex: FIELD — entity A's @Parent field references entity B, so both route to
 * the same worker for strong ordering. The aggregate effect of entangling the schema
 * reduces parallelism, which is the developer's trade-off to manage.
 *
 * Configured via `frames.group_operations_by` as a Set. Once tested, per-OperationSet
 * and per-Entity annotation overrides will allow more granular composition.
 */
enum class AffinityScopeType {
    ENTITY,         // Operation's own entityId claims a worker slot
    TARGETS,        // Entity IDs discovered in the operation's delta
    OPERATION_SET,  // All members of an OperationSet route together
    FIELD,          // Entity IDs in relationship fields (@Parent, @Child, @Peer)
}
