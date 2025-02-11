package com.asyncloadtest.persistence;

import io.vertx.core.json.JsonObject;
import java.util.stream.Stream;

public interface EntityStore extends Store {
    void createEntity(String entityId, String type, JsonObject state);
    void updateEntity(String entityId, long version, JsonObject state);
    JsonObject getEntity(String entityId);
    Stream<JsonObject> getEntitiesByType(String type);
}