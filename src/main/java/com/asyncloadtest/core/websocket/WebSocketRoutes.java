// websocket/WebSocketRoutes.java
package com.asyncloadtest.core.websocket;

import io.vertx.core.http.ServerWebSocket;
import io.vertx.ext.web.Router;
import javax.inject.Inject;

public class WebSocketRoutes {
    private final WebSocketManager webSocketManager;

    @Inject
    public WebSocketRoutes(WebSocketManager webSocketManager) {
        this.webSocketManager = webSocketManager;
    }

    public void register(Router router) {
        router.route("/ws/*").handler(rc -> {
            rc.request().toWebSocket().onSuccess(webSocketManager).onFailure(err -> {
                rc.response().setStatusCode(400).end("WebSocket upgrade failed");
            });
        });
    }
}