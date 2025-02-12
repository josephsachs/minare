package com.minare.core.state;

import com.minare.persistence.ContextStore;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import io.vertx.core.json.JsonArray;
import io.vertx.ext.mongo.MongoClient;
import com.minare.core.websocket.WebSocketManager;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;

import javax.inject.Inject;
import javax.inject.Singleton;

@Slf4j
@Singleton
public class MongoChangeStreamConsumer {
    private final MongoClient mongoClient;
    private final WebSocketManager webSocketManager;
    private final ContextStore contextStore;
    private final Vertx vertx;
    private static final String COLLECTION_NAME = "entities";

    @Inject
    public MongoChangeStreamConsumer(
            MongoClient mongoClient,
            WebSocketManager webSocketManager,
            ContextStore contextStore,
            Vertx vertx) {
        this.mongoClient = mongoClient;
        this.webSocketManager = webSocketManager;
        this.contextStore = contextStore;
        this.vertx = vertx;
    }

    public void startConsuming() {
        JsonArray pipeline = new JsonArray()
                .add(new JsonObject()
                        .put("$match", new JsonObject()
                                .put("operationType", new JsonObject()
                                        .put("$in", new JsonArray().add("update").add("insert"))
                                )));

        mongoClient.createCollection(COLLECTION_NAME)
                .map(v -> mongoClient.watch(
                        COLLECTION_NAME,
                        pipeline,
                        true,
                        1000
                ))
                .onSuccess(stream -> {
                    stream.handler(this::processChange);
                    stream.exceptionHandler(error -> {
                        log.error("Error in change stream", error);
                    });
                })
                .onFailure(error -> {
                    log.error("Failed to start change stream", error);
                });
    }

    private void processChange(ChangeStreamDocument<JsonObject> changeEvent) {
        OperationType operationType = changeEvent.getOperationType();
        if (operationType == OperationType.UPDATE || operationType == OperationType.INSERT) {
            JsonObject fullDocument = changeEvent.getFullDocument();
            if (fullDocument == null) {
                log.warn("No full document in change event");
                return;
            }

            String entityId = fullDocument.getString("_id");
            JsonObject updateMessage = new JsonObject()
                    .put("type", "entityUpdate")
                    .put("entityId", entityId)
                    .put("version", fullDocument.getLong("version"))
                    .put("state", fullDocument.getJsonObject("state"));

            contextStore.getChannelsForEntity(entityId)
                    .onSuccess(channels -> {
                        channels.forEach(channel -> {
                            String channelId = channel.getString("channelId");
                            webSocketManager.broadcastToChannel(channelId, updateMessage);
                        });
                    })
                    .onFailure(error -> {
                        log.error("Failed to get channels for entity {}", entityId, error);
                    });
        }
    }
}