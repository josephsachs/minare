package com.minare.persistence;

import io.vertx.core.Future;

public interface ConnectionStore extends Store {
    Future<Void> storeConnection(String connectionId, long timestamp);
    Future<Void> removeConnection(String connectionId);
    Future<Boolean> isConnectionActive(String connectionId);
}