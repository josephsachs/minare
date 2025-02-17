package com.minare.core.websocket;

import com.minare.core.models.Connection;
import com.minare.persistence.ConnectionStore;
import com.minare.persistence.UserStore;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.http.ServerWebSocket;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;

@Slf4j
@Singleton
public class WebSocketManager implements Handler<ServerWebSocket> {
    private final ConnectionStore connectionStore;
    private final UserStore userStore;
    private final ConnectionManager connectionManager;

    @Inject
    public WebSocketManager(
            ConnectionStore connectionStore,
            UserStore userStore,
            ConnectionManager connectionManager) {
        this.connectionStore = connectionStore;
        this.userStore = userStore;
        this.connectionManager = connectionManager;
    }

    @Override
    public void handle(ServerWebSocket websocket) {
        log.info("New WebSocket connection from {}", websocket.remoteAddress());

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
        switch (type) {
            case "handshake":
                handleHandshake(websocket, message);
                break;
            case "update":
                handleUpdate(websocket, message);
                break;
            default:
                handleError(websocket,
                        new IllegalArgumentException("Unknown message type: " + type));
                break;
        }
    }

    private void handleHandshake(ServerWebSocket websocket, JsonObject message) {
        String userId = message.getString("userId");
        if (userId == null) {
            handleError(websocket, new IllegalStateException("No user ID provided"));
            return;
        }

        userStore.find(userId)
            .compose(user -> {
                String existingConnectionId = user.connectionId;

                if (existingConnectionId != null) {
                    // User already has a connection
                    return connectionManager.getWebSocket(existingConnectionId)
                            .compose(existingSocket -> {
                                if (existingSocket != null) {
                                    existingSocket.close();
                                }
                                return connectionStore.refresh(userId, existingConnectionId)
                                        .compose(connection ->
                                                connectionManager.registerWebSocket(existingConnectionId, websocket)
                                                        .map(v -> connection)
                                        );
                            });
                } else {
                    // Create new connection
                    return connectionStore.create(user.getId())
                            .flatMap(connection ->
                                    connectionManager.registerWebSocket(connection.id, websocket)
                                            .map(v -> connection)
                            )
                            .flatMap(connection ->
                                    userStore.update(user.getId(), connection.id)
                                            .map(v -> connection)
                            );
                }
            })
            .onSuccess(connection -> sendHandshakeConfirmation(websocket, connection.id))
            .onFailure(err -> handleError(websocket, err));
    }

    private void sendHandshakeConfirmation(ServerWebSocket websocket, String connectionId) {
        JsonObject confirmation = new JsonObject()
                .put("type", "handshake_confirm")
                .put("connectionId", connectionId)
                .put("timestamp", System.currentTimeMillis());

        websocket.writeTextMessage(confirmation.encode());
        log.info("Handshake completed for connection {}", connectionId);
    }

    private void handleUpdate(ServerWebSocket websocket, JsonObject message) {
        // Stub for now - to be redesigned
        log.debug("Update received: {}", message);
    }

    private void handleClose(ServerWebSocket websocket) {
        connectionManager.getConnectionIdForWebSocket(websocket)
                .compose(connectionId -> {
                    if (connectionId != null) {
                        return connectionManager.removeWebSocket(connectionId)
                                .compose(v -> connectionStore.delete(connectionId));
                    }
                    return Future.succeededFuture();
                })
                .onFailure(err -> log.error("Error handling websocket close", err));
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