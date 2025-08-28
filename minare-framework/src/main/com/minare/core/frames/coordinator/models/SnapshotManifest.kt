package com.minare.core.frames.coordinator.models

import java.io.Serializable

data class SnapshotManifest(
    val workerId: String,
    val entityIds: List<String>,
    val sessionId: Long
) : Serializable