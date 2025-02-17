package com.minare.persistence;

import com.minare.core.models.Connection;
import io.vertx.core.Future;

public interface ConnectionStore extends Store {
    Future<Connection> create(String userId);
    Future<Void> delete(String connectionId);
    Future<Connection> find(String connectionId);
    Future<Boolean> isActive(String connectionId);
    Future<Connection> update(String userId, String connectionId);
    Future<Connection> refresh(String userId, String connectionId);
}