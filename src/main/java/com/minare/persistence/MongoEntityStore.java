package com.minare.persistence;

import com.google.inject.Singleton;
import com.minare.core.models.Entity;
import com.minare.core.entity.EntityFactory;
import com.minare.core.entity.EntityReflector;
import com.minare.core.entity.ReflectionCache;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.BulkOperation;
import io.vertx.ext.mongo.MongoClient;
import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.StreamSupport;
import java.util.stream.Collectors;

/**
 * MongoDB implementation of the EntityStore interface.
 * Handles entity storage, retrieval, and version management.
 */
@Singleton
public class MongoEntityStore implements EntityStore {

    private final MongoClient mongoClient;
    private final String collection;
    private final EntityFactory entityFactory;
    private final EntityReflector entityReflector;

    @Inject
    public MongoEntityStore(MongoClient mongoClient, String collection, EntityFactory entityFactory, EntityReflector entityReflector) {
        this.mongoClient = mongoClient;
        this.collection = collection;
        this.entityFactory = entityFactory;
        this.entityReflector = entityReflector;
    }

    /**
     * Builds a MongoDB aggregation pipeline to retrieve an entity and all its potential ancestors.
     *
     * @param entityId The ID of the entity to start with
     * @return A JsonObject containing the aggregation pipeline
     */
    public JsonObject buildAncestorTraversalQuery(String entityId) {
        // Get all parent reference field paths from the reflection cache
        List<String> parentRefPaths = entityReflector.getAllParentReferenceFieldPaths();

        // Build the aggregation pipeline
        JsonArray pipeline = new JsonArray()
                // Stage 1: Match the starting entity
                .add(new JsonObject()
                        .put("$match", new JsonObject()
                                .put("_id", entityId)))

                // Stage 2: Use $graphLookup to find all potential ancestors
                .add(new JsonObject()
                        .put("$graphLookup", new JsonObject()
                                .put("from", collection)
                                .put("startWith", "$_id")
                                .put("connectFromField", "_id")
                                .put("connectToField", parentRefPaths.size() == 1 ? parentRefPaths.get(0) : "state.*")
                                .put("as", "ancestors")
                                .put("maxDepth", 10)
                                .put("depthField", "depth")));

        return new JsonObject().put("pipeline", pipeline);
    }

    /**
     * Executes the ancestor traversal query and returns a Future with the results.
     *
     * @param entityId The ID of the entity to start with
     * @return A Future containing the aggregation results
     */
    private Future<JsonArray> getAncestorsRaw(String entityId) {
        JsonObject query = buildAncestorTraversalQuery(entityId);
        Promise<JsonArray> promise = Promise.promise();
        JsonArray results = new JsonArray();

        mongoClient.aggregate(collection, query.getJsonArray("pipeline"))
                .handler(item -> results.add(item))
                .endHandler(v -> promise.complete(results))
                .exceptionHandler(promise::fail);

        return promise.future();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<Graph<Entity, DefaultEdge>> getAncestorGraph(String entityId) {
        return getAncestorsRaw(entityId)
                .map(this::transformResultsToEntityGraph);
    }

    /**
     * Transforms MongoDB aggregation results into a graph of entity instances.
     *
     * @param results The MongoDB aggregation results
     * @return A directed graph where vertices are entity instances and edges represent parent-child relationships
     */
    public Graph<Entity, DefaultEdge> transformResultsToEntityGraph(JsonArray results) {
        if (results.isEmpty()) {
            return new DefaultDirectedGraph<>(DefaultEdge.class);
        }

        Graph<Entity, DefaultEdge> graph = new DefaultDirectedGraph<>(DefaultEdge.class);

        // First pass: create all entity instances
        Map<String, Entity> entitiesById = StreamSupport.stream(results.spliterator(), false)
                .map(obj -> (JsonObject) obj)
                .flatMap(json -> {
                    // Add the root entity
                    JsonObject rootJson = new JsonObject()
                            .put("_id", json.getString("_id"))
                            .put("type", json.getString("type"))
                            .put("version", json.getInteger("version", 1));

                    // Add all ancestors
                    JsonArray ancestors = json.getJsonArray("ancestors", new JsonArray());

                    return StreamSupport.stream(
                            new Iterable<JsonObject>() {
                                @Override
                                public java.util.Iterator<JsonObject> iterator() {
                                    return new java.util.Iterator<JsonObject>() {
                                        private boolean rootReturned = false;
                                        private int ancestorIndex = 0;

                                        @Override
                                        public boolean hasNext() {
                                            return !rootReturned || ancestorIndex < ancestors.size();
                                        }

                                        @Override
                                        public JsonObject next() {
                                            if (!rootReturned) {
                                                rootReturned = true;
                                                return rootJson;
                                            } else {
                                                return ancestors.getJsonObject(ancestorIndex++);
                                            }
                                        }
                                    };
                                }
                            }.spliterator(),
                            false);
                })
                .collect(Collectors.toMap(
                        json -> json.getString("_id"),
                        json -> {
                            String type = json.getString("type");
                            Entity entity = entityFactory.getNew(type);
                            entity._id = json.getString("_id");
                            entity.version = json.getInteger("version", 1);
                            graph.addVertex(entity);
                            return entity;
                        },
                        (e1, e2) -> e1 // In case of duplicate IDs, keep the first one
                ));

        // Second pass: connect entities
        StreamSupport.stream(results.spliterator(), false)
                .map(obj -> (JsonObject) obj)
                .forEach(json -> {
                    String sourceId = json.getString("_id");
                    String sourceType = json.getString("type");
                    Entity sourceEntity = entitiesById.get(sourceId);

                    if (sourceEntity == null) {
                        return;
                    }

                    // Get reflection cache for this entity type
                    ReflectionCache cache = entityReflector.getReflectionCache(sourceType);
                    if (cache == null) {
                        return;
                    }

                    // For each parent reference field defined in the reflection cache
                    cache.getParentReferenceFields().forEach(parentField -> {
                        String fieldName = parentField.getFieldName();

                        // Get the value from the state object
                        JsonObject state = json.getJsonObject("state", new JsonObject());
                        Object fieldValue = state.getValue(fieldName);

                        // Get the parent ID if this is a reference
                        String parentId = extractParentId(fieldValue);

                        if (parentId != null && entitiesById.containsKey(parentId)) {
                            Entity targetEntity = entitiesById.get(parentId);
                            graph.addEdge(sourceEntity, targetEntity);
                        }
                    });
                });

        return graph;
    }

    /**
     * Extracts a parent entity ID from a field value.
     *
     * @param fieldValue The field value to extract from
     * @return The parent ID, or null if not found
     */
    private String extractParentId(Object fieldValue) {
        if (fieldValue instanceof String) {
            return (String) fieldValue;
        } else if (fieldValue instanceof JsonObject) {
            JsonObject refObject = (JsonObject) fieldValue;
            if (refObject.containsKey("$id")) {
                return refObject.getString("$id");
            }
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Future<JsonObject> updateVersions(Set<String> entityIds) {
        if (entityIds.isEmpty()) {
            return Future.succeededFuture(new JsonObject());
        }

        List<BulkOperation> operations = entityIds.stream()
                .map(id -> {
                    JsonObject filter = new JsonObject().put("_id", id);
                    JsonObject update = new JsonObject().put("$inc", new JsonObject().put("version", 1));
                    return BulkOperation.createUpdate(filter, update);
                })
                .collect(Collectors.toList());

        return mongoClient.bulkWrite(collection, operations)
                .map(result -> {
                    // Transform MongoDB result to your standard format
                    return new JsonObject()
                            .put("updatedCount", result.getModifiedCount())
                            .put("matchedCount", result.getMatchedCount());
                });
    }

    // Additional methods for findById, save, delete, etc. would be here
}