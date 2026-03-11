package com.minare.core.frames.coordinator.models

enum class AffinityScopeType {
    ENTITY,         // Operation's own entityId claims a worker slot
    TARGETS,        // Entity IDs discovered in the operation's delta
    OPERATION_SET,  // All members of an OperationSet route together
    FIELD,          // Entity IDs in relationship fields (@Parent, @Child, @Peer)
}
