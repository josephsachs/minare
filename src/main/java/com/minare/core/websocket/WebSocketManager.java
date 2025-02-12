// websocket/WebSocketManager.java
package com.minare.core.websocket;

import com.minare.controller.AbstractEntityController;
import com.minare.persistence.ContextStore;
import io.vertx.core.Handler;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;
import java.util.UUID;

@Slf4j
@Singleton
public class WebSocketManager implements Handler<ServerWebSocket> {
    private final AbstractEntityController entityController;
    private final ConnectionManager connectionManager;
    private final ContextStore contextStore;

    @Inject
    public WebSocketManager(
            AbstractEntityController entityController,
            ConnectionManager connectionManager,
            ContextStore contextStore) {
        this.entityController = entityController;
        this.connectionManager = connectionManager;
        this.contextStore = contextStore;
    }

    @Override
    public void handle(ServerWebSocket websocket) {
        log.info("New WebSocket connection from {}", websocket.remoteAddress());

        // Set up the message handler before doing anything else
        websocket.textMessageHandler(message -> {
            try {
                JsonObject msg = new JsonObject(message);
                handleMessage(websocket, msg);
            } catch (Exception e) {
                handleError(websocket, e);
            }
        });

        websocket.closeHandler(v -> handleClose(websocket));
        websocket.accept();
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
        try {
            // Register connection and get server-generated ID
            String connectionId = connectionManager.registerConnection(websocket);

            // Send confirmation with connection ID
            JsonObject confirmation = new JsonObject()
                    .put("type", "handshake_confirm")
                    .put("connectionId", connectionId)
                    .put("timestamp", System.currentTimeMillis());

            websocket.writeTextMessage(confirmation.encode());
            log.info("Handshake completed for connection {}", connectionId);

        } catch (Exception e) {
            log.error("Handshake failed", e);
            handleError(websocket, e);
        }
    }

    private void handleUpdate(ServerWebSocket websocket, JsonObject message) {
        try {
            String connectionId = message.getString("connectionId");
            if (connectionId == null) {
                throw new IllegalStateException("No connection ID provided");
            }

            ServerWebSocket registeredSocket = connectionManager.getWebSocket(connectionId);
            if (registeredSocket != websocket) {
                throw new IllegalStateException("Invalid connection ID");
            }

            // Process the update
            JsonObject update = message.getJsonObject("state");
            String entityId = (message.getString("entityId"));

            if (entityId == null) {
                // Create one and give it to the Connection
                entityId = String.valueOf(UUID.randomUUID());
                // Grant it to the Connection that created it
            } else {
                // Get the Connection by connectionId
                // if (connection.user.userCanUpdate(entityId))
                // Get the Entity by entityId
                // Delegate to the implementation to operate on the Entity
                entityId = "the entity id goes here";
            }

            long version = message.getLong("version");

            entityController.handleUpdate(entityId, update, version);

        } catch (Exception e) {
            handleError(websocket, e);
        }
    }

    private void handleClose(ServerWebSocket websocket) {
        String connectionId = connectionManager.getConnectionIdForWebSocket(websocket);

        if (connectionId != null) {
            connectionManager.removeConnection(connectionId);
            log.info("WebSocket connection closed: {}", connectionId);
        } else {
            log.warn("Closed websocket had no associated connectionId");
        }
    }

    public void broadcastToChannel(String channelId, JsonObject message) {
        contextStore.getEntitiesInChannel(channelId);

        String connectionId = message.getString("connectionId");
        ServerWebSocket websocket = connectionManager.getWebSocket(connectionId);
        if (websocket != null) {
            websocket.writeTextMessage(message.encode());
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