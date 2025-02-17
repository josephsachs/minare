package com.minare.persistence;

import com.minare.core.models.AbstractEntity;
import com.minare.core.models.Channel;
import com.minare.core.models.annotations.Entity;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;

import java.util.List;

public interface ContextStore extends Store {
    Future<Void> addEntityToChannel(String entityId, String channelId);
    Future<Void> removeEntityFromChannel(String entityId, String channelId);
    Future<List<Channel>> getChannelsForEntity(String entityId);
    Future<List<AbstractEntity>> getEntitiesInChannel(String channelId);
}
