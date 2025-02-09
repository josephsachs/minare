// core/websocket/ConnectionManager.java
package com.asyncloadtest.core.websocket;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.model.*;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import io.vertx.core.http.ServerWebSocket;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Singleton
public class ConnectionManager {
    private static final String SUBSCRIPTIONS_TABLE = "Subscriptions";
    private final AmazonDynamoDB dynamoDB;
    private final IMap<String, Map<String, String>> activeConnections;
    private final Map<String, ServerWebSocket> websockets = new ConcurrentHashMap<>();

    @Inject
    public ConnectionManager(AmazonDynamoDB dynamoDB, HazelcastInstance hazelcast) {
        this.dynamoDB = dynamoDB;
        this.activeConnections = hazelcast.getMap("activeConnections");
    }

    public void registerConnection(String userId, String channelId, ServerWebSocket websocket) {
        String connectionId = websocket.textHandlerID();
        long timestamp = System.currentTimeMillis();

        // Store connection in DynamoDB
        Map<String, AttributeValue> item = new HashMap<>();
        item.put("Timestamp", new AttributeValue().withN(String.valueOf(timestamp)));
        item.put("Channel", new AttributeValue(channelId));
        item.put("UserId", new AttributeValue(userId));
        item.put("ConnectionId", new AttributeValue(connectionId));

        try {
            dynamoDB.putItem(new PutItemRequest()
                    .withTableName(SUBSCRIPTIONS_TABLE)
                    .withItem(item));
        } catch (Exception e) {
            log.error("Failed to store connection in DynamoDB", e);
            throw e;
        }

        // Store in Hazelcast
        Map<String, String> connectionDetails = new HashMap<>();
        connectionDetails.put("userId", userId);
        connectionDetails.put("channelId", channelId);
        activeConnections.put(connectionId, connectionDetails);

        // Store websocket reference
        websockets.put(connectionId, websocket);

        log.info("Registered connection {} for user {} in channel {}", connectionId, userId, channelId);
    }

    public void removeConnection(String connectionId) {
        // Remove from DynamoDB
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("ConnectionId", new AttributeValue(connectionId));

        try {
            dynamoDB.deleteItem(new DeleteItemRequest()
                    .withTableName(SUBSCRIPTIONS_TABLE)
                    .withKey(key));
        } catch (Exception e) {
            log.error("Failed to remove connection from DynamoDB", e);
        }

        // Remove from Hazelcast
        activeConnections.remove(connectionId);

        // Remove websocket reference
        websockets.remove(connectionId);

        log.info("Removed connection {}", connectionId);
    }

    public ServerWebSocket getWebSocket(String connectionId) {
        return websockets.get(connectionId);
    }

    public Map<String, String> getConnectionDetails(String connectionId) {
        return activeConnections.get(connectionId);
    }

    public void cleanupStaleConnections() {
        long cutoffTime = System.currentTimeMillis() - (12 * 1000); // 12 seconds

        // Query DynamoDB for stale connections
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":cutoff", new AttributeValue().withN(String.valueOf(cutoffTime)));

        ScanRequest scanRequest = new ScanRequest()
                .withTableName(SUBSCRIPTIONS_TABLE)
                .withFilterExpression("Timestamp < :cutoff")
                .withExpressionAttributeValues(expressionAttributeValues);

        try {
            ScanResult result = dynamoDB.scan(scanRequest);
            for (Map<String, AttributeValue> item : result.getItems()) {
                String connectionId = item.get("ConnectionId").getS();
                removeConnection(connectionId);
            }
        } catch (Exception e) {
            log.error("Failed to cleanup stale connections", e);
        }
    }
}