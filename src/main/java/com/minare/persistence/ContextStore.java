package com.minare.persistence;

import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.List;

public interface ContextStore extends Store {
    Future<Void> addEntityToChannel(String entityId, String channelId);
    Future<Void> removeEntityFromChannel(String entityId, String channelId);
    Future<List<JsonObject>> getChannelsForEntity(String entityId);
    Future<List<JsonObject>> getEntitiesInChannel(String channelId);
}
