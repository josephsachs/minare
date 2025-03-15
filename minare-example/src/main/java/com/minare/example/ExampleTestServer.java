// example/ExampleTestServer.java
package com.minare.example;

import com.google.inject.Singleton;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.mongo.MongoClient;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.StaticHandler;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;

@Slf4j
@Singleton
public class ExampleTestServer {
    private final MongoClient mongoClient;
    private static final String COLLECTION_NAME = "users";

    @Inject
    public ExampleTestServer(MongoClient mongoClient) {
        this.mongoClient = mongoClient;
    }

    public Future<Nothing?> initializeTestUser() {
        JsonObject user = new JsonObject()
                .put("_id", "1")
                .put("ownedEntityIds", new JsonArray());

        JsonObject query = new JsonObject().put("_id", "1");

        return mongoClient.findOne(COLLECTION_NAME, query, null)
                .compose(existingUser -> {
                    if (existingUser == null) {
                        log.info("Creating test user");
                        return mongoClient.insert(COLLECTION_NAME, user);
                    }
                    log.info("Test user already exists");
                    return Future.succeededFuture();
                })
                .mapEmpty();
    }

    public void configureRoutes(Router router) {
        StaticHandler staticHandler = StaticHandler.create()
                .setWebRoot("src/main/resources/example.webroot");
        router.route("/*").handler(staticHandler);

        log.debug("Static routes configured for example server");
    }
}