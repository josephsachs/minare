package com.asyncloadtest.persistence;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.stream.Stream;

public interface EntityStore {
    Future<Void> createEntity(String entityId, String type, JsonObject state);
    Future<Long> updateEntity(String entityId, long version, JsonObject state);
    Future<JsonObject> getEntity(String entityId);
    Future<Stream<JsonObject>> getEntitiesByType(String type);
}