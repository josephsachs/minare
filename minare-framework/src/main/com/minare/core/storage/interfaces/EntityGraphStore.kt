package com.minare.core.storage.interfaces

import com.minare.core.entity.models.Entity
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge

interface EntityGraphStore {
    suspend fun save(entity: Entity): Entity
    suspend fun updateRelationships(entityId: String, delta: JsonObject): JsonObject
    suspend fun updateVersions(entityIds: Set<String>): JsonObject
    suspend fun findEntitiesByIds(entityIds: List<String>): Map<String, Entity>
    suspend fun getAncestorGraph(entityId: String): Graph<Entity, DefaultEdge>
    suspend fun buildEntityGraph(entityIds: List<String>): Graph<Entity, DefaultEdge>
    suspend fun buildDocumentGraph(entityIds: List<String>): Graph<JsonObject, DefaultEdge>
    fun traverseParents(
        graph: Graph<JsonObject, DefaultEdge>,
        document: JsonObject,
        visited: MutableSet<String>
    ): List<JsonObject>
}