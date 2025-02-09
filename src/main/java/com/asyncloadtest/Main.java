// Main.java
package com.asyncloadtest;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.asyncloadtest.config.GuiceModule;
import com.asyncloadtest.core.websocket.WebSocketManager;
import com.asyncloadtest.core.websocket.WebSocketRoutes;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;

// Import the module
import com.asyncloadtest.example.ExampleTestServer;

@Slf4j
public class Main {
    public static void main(String[] args) {
        Injector injector = Guice.createInjector(new GuiceModule());

        Vertx vertx = injector.getInstance(Vertx.class);
        WebSocketManager webSocketManager = injector.getInstance(WebSocketManager.class);
        WebSocketRoutes wsRoutes = injector.getInstance(WebSocketRoutes.class);

        // Register the module
        ExampleTestServer exampleServer = injector.getInstance(ExampleTestServer.class);

        Router router = Router.router(vertx);

        // Register routes
        wsRoutes.register(router);

        // Register module roots
        exampleServer.configureRoutes(router);

        // Setup server
        vertx.createHttpServer()
                .requestHandler(router)
                .listen(8080, http -> {
                    if (http.succeeded()) {
                        log.info("Server started on port 8080");
                    } else {
                        log.error("Failed to start server", http.cause());
                        System.exit(1);
                    }
                });
    }
}