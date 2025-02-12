package com.minare;

import com.minare.persistence.DatabaseInitializer;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.minare.config.GuiceModule;
import com.minare.core.state.MongoChangeStreamConsumer;
import com.minare.core.websocket.WebSocketManager;
import com.minare.core.websocket.WebSocketRoutes;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import lombok.extern.slf4j.Slf4j;

import com.minare.example.ExampleTestServer;

@Slf4j
public class Main {
    public static void main(String[] args) {
        Injector injector = Guice.createInjector(new GuiceModule());

        Vertx vertx = injector.getInstance(Vertx.class);
        WebSocketManager webSocketManager = injector.getInstance(WebSocketManager.class);
        WebSocketRoutes wsRoutes = injector.getInstance(WebSocketRoutes.class);

        DatabaseInitializer dbInitializer = injector.getInstance(DatabaseInitializer.class);
        dbInitializer.initialize();
        MongoChangeStreamConsumer changeStreamConsumer = injector.getInstance(MongoChangeStreamConsumer.class);
        changeStreamConsumer.startConsuming();

        Router router = Router.router(vertx);
        wsRoutes.register(router);

        // Register project modules
        ExampleTestServer exampleServer = injector.getInstance(ExampleTestServer.class);
        exampleServer.configureRoutes(router);

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