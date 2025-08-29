package com.minare.core.storage.interfaces

import com.minare.core.entity.models.Entity
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultEdge

interface EntityQueryStore {
    suspend fun findEntitiesByIds(entityIds: List<String>): Map<String, Entity>
    suspend fun buildEntityGraph(entityIds: List<String>): Graph<Entity, DefaultEdge>
    suspend fun buildDocumentGraph(entityIds: List<String>): Graph<JsonObject, DefaultEdge>
    suspend fun getAncestorGraph(entityId: String): Graph<Entity, DefaultEdge>
    suspend fun updateVersions(entityIds: Set<String>): JsonObject
    fun traverseParents(graph: Graph<JsonObject, DefaultEdge>, document: JsonObject, visited: MutableSet<String> = mutableSetOf()): List<JsonObject>
}