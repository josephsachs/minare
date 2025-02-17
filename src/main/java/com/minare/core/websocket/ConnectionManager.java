package com.minare.core.websocket;

import io.vertx.codegen.annotations.Nullable;
import io.vertx.core.AsyncResult;
import io.vertx.core.Vertx;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.shareddata.AsyncMap;
import io.vertx.core.shareddata.SharedData;
import lombok.extern.slf4j.Slf4j;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import io.vertx.core.Future;

@Slf4j
@Singleton
public class ConnectionManager {
    private final Vertx vertx;
    private final Map<String, ServerWebSocket> localConnections;
    private static final String MAP_NAME = "websocket.connections";

    @Inject
    public ConnectionManager(Vertx vertx) {
        this.vertx = vertx;
        this.localConnections = new ConcurrentHashMap<>();
    }

    public Future<Void> registerWebSocket(String connectionId, ServerWebSocket websocket) {
        localConnections.put(connectionId, websocket);
        log.info("Registered websocket for connection {} on local instance", connectionId);
        return Future.succeededFuture();
    }

    public Future<@Nullable ServerWebSocket> removeWebSocket(String connectionId) {
        ServerWebSocket removed = localConnections.remove(connectionId);
        log.info("Removed websocket for connection {} from local instance", connectionId);
        return Future.succeededFuture(removed);
    }

    public Future<ServerWebSocket> getWebSocket(String connectionId) {
        return Future.succeededFuture(localConnections.get(connectionId));
    }

    public Future<String> getConnectionIdForWebSocket(ServerWebSocket websocket) {
        return Future.succeededFuture(
                localConnections.entrySet().stream()
                        .filter(entry -> entry.getValue() == websocket)
                        .map(Map.Entry::getKey)
                        .findFirst()
                        .orElse(null)
        );
    }
}