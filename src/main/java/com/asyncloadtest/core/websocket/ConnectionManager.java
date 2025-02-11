package com.asyncloadtest.core.websocket;

import com.asyncloadtest.persistence.ConnectionStore;
import io.vertx.core.http.ServerWebSocket;
import lombok.extern.slf4j.Slf4j;
import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;

@Slf4j
@Singleton
public class ConnectionManager {
    private final ConnectionStore connectionStore;
    private final Map<String, ServerWebSocket> websockets = new ConcurrentHashMap<>();

    @Inject
    public ConnectionManager(ConnectionStore connectionStore) {
        this.connectionStore = connectionStore;
    }

    public String registerConnection(ServerWebSocket websocket) {
        String connectionId = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();

        connectionStore.storeConnection(connectionId, timestamp);
        websockets.put(connectionId, websocket);

        log.info("Registered connection {}", connectionId);
        return connectionId;
    }

    public void removeConnection(String connectionId) {
        connectionStore.removeConnection(connectionId);
        websockets.remove(connectionId);
        log.info("Removed connection {}", connectionId);
    }

    public ServerWebSocket getWebSocket(String connectionId) {
        return websockets.get(connectionId);
    }

    public String getConnectionIdForWebSocket(ServerWebSocket websocket) {
        return websockets.entrySet().stream()
                .filter(entry -> entry.getValue() == websocket)
                .map(Map.Entry::getKey)
                .findFirst()
                .orElse(null);
    }
}