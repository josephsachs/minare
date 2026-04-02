package com.minare.core.entity.graph

import com.google.inject.Inject
import com.google.inject.Singleton
import com.minare.core.entity.annotations.Child
import com.minare.core.entity.annotations.Parent
import com.minare.core.entity.annotations.Peer
import com.minare.core.entity.factories.EntityFactory
import com.minare.core.entity.services.EntityInspector
import com.minare.core.storage.adapters.MongoEntityStore
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import org.jgrapht.Graph
import org.jgrapht.graph.DefaultDirectedGraph
import org.jgrapht.graph.DefaultEdge
import org.slf4j.LoggerFactory
import kotlin.reflect.KClass

/**
 * Builds subgraphs of the entity relationship graph by traversing from a
 * starting entity along configured relationship types, using MongoDB's
 * $graphLookup for greedy server-side expansion.
 *
 * A predicate function lets the caller prune the result set after expansion,
 * so domain logic can restrict which entities make it into the final graph.
 *
 * Requires an entity graph store (Mongo) to be configured.
 */
@Singleton
class EntityGraphVisitorBuilder @Inject constructor(
    private val mongoEntityStore: MongoEntityStore,
    private val entityInspector: EntityInspector,
    private val entityFactory: EntityFactory
) {
    private val log = LoggerFactory.getLogger(EntityGraphVisitorBuilder::class.java)

    companion object {
        private const val COLLECTION = "entity_graph"
        private const val DEFAULT_MAX_DEPTH = 10
    }

    enum class RelationshipType(val annotationClass: KClass<out Annotation>) {
        PARENT(Parent::class),
        CHILD(Child::class),
        PEER(Peer::class)
    }

    /**
     * Visit the entity graph starting from [entityId], traversing along the
     * specified [relationshipTypes]. The greedy result from $graphLookup is
     * pruned by [predicate] before the JGraphT graph is built.
     *
     * @param entityId      Starting entity
     * @param relationshipTypes Which relationship directions to follow
     * @param maxDepth      Maximum traversal depth (default 10)
     * @param predicate     Receives each candidate entity document; return true to keep
     * @return Directed graph of entity documents that passed the predicate
     */
    suspend fun visit(
        entityId: String,
        relationshipTypes: Set<RelationshipType>,
        maxDepth: Int = DEFAULT_MAX_DEPTH,
        predicate: (JsonObject) -> Boolean = { true }
    ): Graph<JsonObject, DefaultEdge> {
        require(relationshipTypes.isNotEmpty()) { "At least one relationship type is required" }

        val rootDoc = mongoEntityStore.fetchDocumentsByIds(listOf(entityId)).firstOrNull()
        if (rootDoc == null) {
            log.warn("Entity {} not found in graph store", entityId)
            return DefaultDirectedGraph(DefaultEdge::class.java)
        }

        val connectFromFields = resolveAllConnectFromFields(relationshipTypes)
        if (connectFromFields.isEmpty()) {
            log.debug("No relationship fields found across any entity type for requested relationship types")
            return singleNodeGraph(rootDoc, predicate)
        }

        val allDocuments = executeLookups(entityId, connectFromFields, maxDepth)

        val filtered = allDocuments.filter(predicate)
        if (filtered.isEmpty()) {
            return DefaultDirectedGraph(DefaultEdge::class.java)
        }

        return buildGraph(filtered, relationshipTypes)
    }

    /**
     * Collects all distinct `state.*` field names across every registered entity
     * type that carry one of the requested relationship annotations.
     *
     * This lets $graphLookup cross type boundaries: a traversal starting on a
     * Node with `parentId` will continue through a Graph entity that uses
     * `ownerId`, as long as both are @Parent fields and PARENT is requested.
     */
    private fun resolveAllConnectFromFields(
        relationshipTypes: Set<RelationshipType>
    ): List<String> {
        val annotationClasses = relationshipTypes.map { it.annotationClass }
        val fieldNames = mutableSetOf<String>()

        for (typeName in entityFactory.getTypeNames()) {
            val fields = entityInspector.getFieldsOfType(typeName, annotationClasses)
            fields.mapTo(fieldNames) { "state.${it.name}" }
        }

        return fieldNames.toList()
    }

    /**
     * Runs one $graphLookup per connectFrom field and merges the results.
     *
     * MongoDB's $graphLookup only accepts a single connectFromField, so when
     * traversing multiple relationship fields (e.g. parentId + childIds) we
     * issue one pipeline per field and deduplicate by _id.
     */
    private suspend fun executeLookups(
        entityId: String,
        connectFromFields: List<String>,
        maxDepth: Int
    ): List<JsonObject> {
        val documentsById = mutableMapOf<String, JsonObject>()

        for (connectFromField in connectFromFields) {
            val pipeline = JsonArray().apply {
                add(JsonObject().put("\$match", JsonObject().put("_id", entityId)))

                add(JsonObject().put("\$graphLookup", JsonObject()
                    .put("from", COLLECTION)
                    .put("startWith", "\$$connectFromField")
                    .put("connectFromField", connectFromField)
                    .put("connectToField", "_id")
                    .put("as", "visited")
                    .put("maxDepth", maxDepth)
                    .put("depthField", "depth")
                ))

                add(JsonObject().put("\$project", JsonObject()
                    .put("root", "\$\$ROOT")
                    .put("visited", 1)
                ))
            }

            val results = mongoEntityStore.executeAggregation(pipeline)

            for (i in 0 until results.size()) {
                val result = results.getJsonObject(i)

                // Include the root document
                val rootFields = result.copy().apply { remove("visited") }
                val rootId = rootFields.getString("_id")
                if (rootId != null) {
                    documentsById.putIfAbsent(rootId, rootFields)
                }

                // Include traversed documents
                val visited = result.getJsonArray("visited", JsonArray())
                for (j in 0 until visited.size()) {
                    val doc = visited.getJsonObject(j)
                    val docId = doc.getString("_id")
                    if (docId != null) {
                        // Remove the depth field added by $graphLookup
                        doc.remove("depth")
                        documentsById.putIfAbsent(docId, doc)
                    }
                }
            }
        }

        return documentsById.values.toList()
    }

    /**
     * Builds a JGraphT directed graph from entity documents, adding edges
     * based on the requested relationship types.
     */
    private fun buildGraph(
        documents: List<JsonObject>,
        relationshipTypes: Set<RelationshipType>
    ): Graph<JsonObject, DefaultEdge> {
        val graph = DefaultDirectedGraph<JsonObject, DefaultEdge>(DefaultEdge::class.java)

        val docsById = mutableMapOf<String, JsonObject>()
        for (doc in documents) {
            graph.addVertex(doc)
            val id = doc.getString("_id")
            if (id != null) {
                docsById[id] = doc
            }
        }

        for (doc in documents) {
            val entityType = doc.getString("type") ?: continue
            val state = doc.getJsonObject("state") ?: continue

            val annotationClasses = relationshipTypes.map { it.annotationClass }
            val fields = entityInspector.getFieldsOfType(entityType, annotationClasses)

            for (field in fields) {
                val value = state.getValue(field.name) ?: continue
                val targetIds = extractEntityIds(value)

                for (targetId in targetIds) {
                    val targetDoc = docsById[targetId] ?: continue
                    if (doc !== targetDoc && !graph.containsEdge(doc, targetDoc)) {
                        graph.addEdge(doc, targetDoc)
                    }
                }
            }
        }

        return graph
    }

    private fun singleNodeGraph(
        doc: JsonObject,
        predicate: (JsonObject) -> Boolean
    ): Graph<JsonObject, DefaultEdge> {
        val graph = DefaultDirectedGraph<JsonObject, DefaultEdge>(DefaultEdge::class.java)
        if (predicate(doc)) {
            graph.addVertex(doc)
        }
        return graph
    }

    private fun extractEntityIds(value: Any?): List<String> {
        return when (value) {
            null -> emptyList()
            is String -> if (value.isNotBlank()) listOf(value) else emptyList()
            is List<*> -> value.filterIsInstance<String>()
            is JsonArray -> value.list.filterIsInstance<String>()
            else -> emptyList()
        }
    }
}