package com.minare.persistence;

import com.minare.core.models.Entity;
import com.minare.core.models.Channel;
import io.vertx.core.Future;

import java.util.List;

public interface ContextStore extends Store {
    Future<Void> addEntityToChannel(String entityId, String channelId);
    Future<Void> removeEntityFromChannel(String entityId, String channelId);
    Future<List<Channel>> getChannelsForEntity(String entityId);
    Future<List<Entity>> getEntitiesInChannel(String channelId);
}
