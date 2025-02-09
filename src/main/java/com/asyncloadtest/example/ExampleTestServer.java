// example/ExampleTestServer.java
package com.asyncloadtest.example;

import com.google.inject.Singleton;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import java.net.URL;

@Slf4j
@Singleton
public class ExampleTestServer {
    @Inject
    public ExampleTestServer() {}

    public void configureRoutes(Router router) {
        StaticHandler staticHandler = StaticHandler.create()
                .setWebRoot("src/main/resources/example.webroot");
        router.route("/*").handler(staticHandler);

        log.debug("Static routes configured for example server");
    }
}