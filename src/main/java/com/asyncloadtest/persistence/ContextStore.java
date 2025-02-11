package com.asyncloadtest.persistence;

import io.vertx.core.json.JsonObject;
import java.util.stream.Stream;

public interface ContextStore extends Store {
    void addEntityToChannel(String entityId, String channelId);
    void removeEntityFromChannel(String entityId, String channelId);
    Stream<JsonObject> getChannelsForEntity(String entityId);
    Stream<JsonObject> getEntitiesInChannel(String channelId);
}
