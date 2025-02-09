// example/ExampleTestServer.java
package com.asyncloadtest.example;

import com.google.inject.Guice;
import com.google.inject.Injector;
import com.asyncloadtest.config.GuiceModule;
import com.asyncloadtest.core.websocket.WebSocketManager;
import io.vertx.core.Vertx;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExampleTestServer {
    public void configureRoutes(Router router) {
        StaticHandler staticHandler = StaticHandler.create()
                .setWebRoot("example/webroot")
                .setCachingEnabled(false)
                .setDirectoryListing(true);
        router.route("/*").handler(staticHandler);
    }
}