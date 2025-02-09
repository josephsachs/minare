package com.asyncloadtest.core.models;

import io.vertx.core.json.JsonObject;
import lombok.Value;

@Value
public class StateUpdate {
    String channelId;
    long timestamp;
    JsonObject state;
    String checksum;

    // Without this, Lombok only generates a no-args constructor
    public StateUpdate(String channelId, long timestamp, JsonObject state, String checksum) {
        this.channelId = channelId;
        this.timestamp = timestamp;
        this.state = state;
        this.checksum = checksum;
    }
}