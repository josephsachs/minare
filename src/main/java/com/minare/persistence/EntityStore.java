package com.minare.persistence;

import com.minare.core.models.AbstractEntity;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

public interface EntityStore {
    Future<AbstractEntity> find(String entityId);
    Future<Set<AbstractEntity>> findAll(Set<String> entityIds);
    Future<Set<AbstractEntity>> findByType(String type);
    Future<AbstractEntity> update(String entityId, JsonObject state);
    AbstractEntity hydrateEntity(JsonObject doc);
}