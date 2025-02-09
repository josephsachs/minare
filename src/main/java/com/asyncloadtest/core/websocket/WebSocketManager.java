// websocket/WebSocketManager.java
package com.asyncloadtest.core.websocket;

import com.asyncloadtest.controller.AbstractEntityController;
import com.asyncloadtest.core.models.StateUpdate;
import com.asyncloadtest.core.websocket.ConnectionManager;
import com.asyncloadtest.core.state.EntityStateManager;
import io.reactivex.rxjava3.disposables.Disposable;
import io.vertx.core.Handler;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
@Singleton
public class WebSocketManager implements Handler<ServerWebSocket> {
    private final AbstractEntityController entityController;
    private final EntityStateManager stateManager;
    private final ConnectionManager connectionManager;
    private final Map<String, Disposable> subscriptions = new ConcurrentHashMap<>();

    @Inject
    public WebSocketManager(
            AbstractEntityController entityController,
            EntityStateManager stateManager,
            ConnectionManager connectionManager) {
        this.entityController = entityController;
        this.stateManager = stateManager;
        this.connectionManager = connectionManager;
    }

    @Override
    public void handle(ServerWebSocket websocket) {
        log.info("New WebSocket connection from {}", websocket.remoteAddress());
        websocket.accept();

        websocket.handler(buffer -> {
            JsonObject message = new JsonObject(buffer.toString());
            handleMessage(websocket, message);
        });

        websocket.closeHandler(v -> handleClose(websocket));
    }

    private void handleMessage(ServerWebSocket websocket, JsonObject message) {
        String type = message.getString("type");
        if ("handshake".equals(type)) {
            handleHandshake(websocket, message);
        } else if ("update".equals(type)) {
            handleUpdate(websocket, message);
        }
    }

    private void handleHandshake(ServerWebSocket websocket, JsonObject message) {
        String userId = message.getString("userId");
        String channelId = message.getString("channelId");

        try {
            // Register connection
            connectionManager.registerConnection(userId, channelId, websocket);

            // Subscribe to state updates
            Disposable subscription = stateManager.subscribeToChannel(channelId)
                    .subscribe(
                            update -> sendUpdate(websocket, update),
                            error -> handleError(websocket, error)
                    );

            subscriptions.put(websocket.textHandlerID(), subscription);

            // Send confirmation
            JsonObject confirmation = new JsonObject()
                    .put("type", "handshake_confirm")
                    .put("userId", userId)
                    .put("channelId", channelId);
            websocket.writeTextMessage(confirmation.encode());

            log.info("Handshake completed for user {} in channel {}", userId, channelId);
        } catch (Exception e) {
            log.error("Handshake failed", e);
            handleError(websocket, e);
        }
    }

    private void handleUpdate(ServerWebSocket websocket, JsonObject message) {
        try {
            String connectionId = websocket.textHandlerID();
            Map<String, String> connectionDetails = connectionManager.getConnectionDetails(connectionId);

            if (connectionDetails == null) {
                throw new IllegalStateException("No active connection found");
            }

            String channelId = message.getString("channelId");
            if (!channelId.equals(connectionDetails.get("channelId"))) {
                throw new IllegalStateException("Channel mismatch");
            }

            JsonObject update = message.getJsonObject("state");
            String checksum = message.getString("checksum");
            long timestamp = message.getLong("timestamp");

            entityController.handleUpdate(channelId, update, checksum, timestamp);
        } catch (Exception e) {
            handleError(websocket, e);
        }
    }

    private void handleClose(ServerWebSocket websocket) {
        String connectionId = websocket.textHandlerID();

        // Clean up subscription
        Disposable subscription = subscriptions.remove(connectionId);
        if (subscription != null) {
            subscription.dispose();
        }

        // Remove connection
        connectionManager.removeConnection(connectionId);

        log.info("WebSocket connection closed: {}", connectionId);
    }

    private void sendUpdate(ServerWebSocket websocket, StateUpdate update) {
        try {
            JsonObject message = new JsonObject()
                    .put("type", "update")
                    .put("channelId", update.getChannelId())
                    .put("state", update.getState())
                    .put("checksum", update.getChecksum())
                    .put("timestamp", update.getTimestamp());

            websocket.writeTextMessage(message.encode());
        } catch (Exception e) {
            log.error("Failed to send update", e);
            handleError(websocket, e);
        }
    }

    private void handleError(ServerWebSocket websocket, Throwable error) {
        log.error("Error in WebSocket connection", error);
        try {
            JsonObject errorMessage = new JsonObject()
                    .put("type", "error")
                    .put("message", error.getMessage());

            websocket.writeTextMessage(errorMessage.encode());
        } finally {
            websocket.close();
        }
    }
}