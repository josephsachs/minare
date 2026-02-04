package com.minare.core.entity.graph

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.entity.models.Entity
import com.minare.core.storage.adapters.MongoEntityStore
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge

@Singleton
class EntityGraphService @Inject constructor(
    private val mongoEntityStore: MongoEntityStore
) {
    companion object {
        private const val MAX_GRAPH_DEPTH = 10
    }
    /**
     * Retrieves all ancestors of an entity using MongoDB's $graphLookup.
     */
    suspend fun getAncestorGraph(entityId: String): Graph<Entity, DefaultEdge> {
        val pipeline = JsonArray().apply {
            // Match the starting entity
            add(JsonObject().put("\$match", JsonObject().put("_id", entityId)))

            // Graph lookup for ancestors
            add(JsonObject().put("\$graphLookup", JsonObject().apply {
                put("from", "entity_graph")
                put("startWith", "\$_id")
                put("connectFromField", "_id")
                put("connectToField", "state.*")
                put("as", "ancestors")
                put("maxDepth", MAX_GRAPH_DEPTH)
                put("depthField", "depth")
            }))
        }

        val results = mongoEntityStore.executeAggregation(pipeline)
        return transformResultsToEntityGraph(results)
    }

    /**
     * Builds a graph of entities based on their relationships.
     */
    suspend fun buildEntityGraph(entityIds: List<String>): Graph<Entity, DefaultEdge> {
        if (entityIds.isEmpty()) {
            return DefaultDirectedGraph(DefaultEdge::class.java)
        }

        val documents = mongoEntityStore.fetchDocumentsByIds(entityIds)
        return buildGraphFromDocuments(documents) { mongoEntityStore.createMinimalEntity(it) }
    }

    /**
     * Builds a graph of raw MongoDB documents.
     */
    suspend fun buildDocumentGraph(entityIds: List<String>): Graph<JsonObject, DefaultEdge> {
        if (entityIds.isEmpty()) {
            return DefaultDirectedGraph(DefaultEdge::class.java)
        }

        val documents = mongoEntityStore.fetchDocumentsByIds(entityIds)
        return buildGraphFromDocuments(documents) { it }
    }


    /**
     * Traverses parent relationships in a document graph.
     */
    fun traverseParents(
        graph: Graph<JsonObject, DefaultEdge>,
        document: JsonObject,
        visited: MutableSet<String> = mutableSetOf()
    ): List<JsonObject> {
        val documentId = document.getString("_id") ?: return emptyList()
        visited.add(documentId)

        return graph.outgoingEdgesOf(document).flatMap { edge ->
            val parent = graph.getEdgeTarget(edge)
            val parentId = parent.getString("_id")

            if (parentId != null && parentId !in visited) {
                listOf(parent) + traverseParents(graph, parent, visited)
            } else {
                emptyList()
            }
        }
    }

    fun transformResultsToEntityGraph(results: JsonArray): Graph<Entity, DefaultEdge> {
        val graph = DefaultDirectedGraph<Entity, DefaultEdge>(DefaultEdge::class.java)

        if (results.isEmpty) {
            return graph
        }

        // Flatten all entities from results
        val allDocuments = results.flatMap { result ->
            val rootDoc = result as JsonObject
            val ancestors = rootDoc.getJsonArray("ancestors", JsonArray())

            listOf(rootDoc.copy().apply { remove("ancestors") }) +
                    ancestors.map { it as JsonObject }
        }.distinctBy { it.getString("_id") }

        return buildGraphFromDocuments(allDocuments) { mongoEntityStore.createMinimalEntity(it) }
    }

    private fun <T> buildGraphFromDocuments(
        documents: List<JsonObject>,
        nodeFactory: (JsonObject) -> T
    ): Graph<T, DefaultEdge> {
        val graph = DefaultDirectedGraph<T, DefaultEdge>(DefaultEdge::class.java)

        if (documents.isEmpty()) {
            return graph
        }

        // Create nodes and build lookup map
        val nodesById = documents.associate { document ->
            val node = nodeFactory(document)
            graph.addVertex(node)
            document.getString("_id") to (node to document)
        }

        // Add edges based on relationships
        nodesById.forEach { (sourceId, nodePair) ->
            val (sourceNode, document) = nodePair
            val state = document.getJsonObject("state", JsonObject())

            // Look for parent relationships
            state.fieldNames().forEach { fieldName ->
                val fieldValue = state.getValue(fieldName)
                mongoEntityStore.extractEntityId(fieldValue)?.let { targetId ->
                    nodesById[targetId]?.let { (targetNode, _) ->
                        graph.addEdge(sourceNode, targetNode)
                    }
                }
            }
        }

        return graph
    }
}