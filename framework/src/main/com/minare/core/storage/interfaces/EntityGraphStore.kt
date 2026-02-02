package com.minare.core.storage.interfaces

import com.minare.core.entity.models.Entity
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge

interface EntityGraphStore {
    suspend fun save(entity: Entity, create: Boolean): Entity
    suspend fun updateRelationships(entityId: String, delta: JsonObject): JsonObject
    suspend fun updateVersions(entityIds: Set<String>): JsonObject
    suspend fun findEntitiesByIds(entityIds: List<String>): Map<String, Entity>
}