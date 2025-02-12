package com.asyncloadtest.persistence;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import java.util.stream.Stream;

public interface ContextStore extends Store {
    Future<Void> addEntityToChannel(String entityId, String channelId);
    Future<Void> removeEntityFromChannel(String entityId, String channelId);
    Future<Stream<JsonObject>> getChannelsForEntity(String entityId);
    Future<Stream<JsonObject>> getEntitiesInChannel(String channelId);
}
