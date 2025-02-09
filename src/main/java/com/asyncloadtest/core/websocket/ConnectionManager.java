package com.asyncloadtest.core.websocket;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
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
    private final IMap<String, Long> activeConnections;
    private final Map<String, ServerWebSocket> websockets = new ConcurrentHashMap<>();

    @Inject
    public ConnectionManager(AmazonDynamoDB dynamoDB, HazelcastInstance hazelcast) {
        this.activeConnections = hazelcast.getMap("activeConnections");
    }

    public String registerConnection(ServerWebSocket websocket) {
        String connectionId = UUID.randomUUID().toString();
        long timestamp = System.currentTimeMillis();

        activeConnections.put(connectionId, timestamp);
        websockets.put(connectionId, websocket);

        log.info("Registered connection {}", connectionId);
        return connectionId;
    }

    public void removeConnection(String connectionId) {
        activeConnections.remove(connectionId);
        websockets.remove(connectionId);
        log.info("Removed connection {}", connectionId);
    }

    public ServerWebSocket getWebSocket(String connectionId) {
        return websockets.get(connectionId);
    }

    public void cleanupStaleConnections() {
        long cutoffTime = System.currentTimeMillis() - (12 * 1000); // 12 seconds

        activeConnections.forEach((connectionId, value) -> {
            if (value < cutoffTime) {
                ServerWebSocket ws = websockets.get(connectionId);
                if (ws != null) {
                    ws.close();
                }
                removeConnection(connectionId);
            }
        });
    }
}