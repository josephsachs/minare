package com.asyncloadtest.core.models;

import io.vertx.core.json.JsonObject;
import lombok.Value;

@Value
public class StateUpdate {
    String entityId;
    long version;
    JsonObject state;
    long timestamp;

    // Without this, Lombok only generates a no-args constructor
    public StateUpdate(String entityId, long timestamp, JsonObject state, long version) {
        this.entityId = entityId;
        this.timestamp = timestamp;
        this.state = state;
        this.version = version;
    }
}