package com.asyncloadtest.core.state;

import com.asyncloadtest.persistence.ContextStore;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.changestream.ChangeStreamDocument;
import com.mongodb.client.model.changestream.OperationType;
import com.asyncloadtest.core.websocket.WebSocketManager;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;

import javax.inject.Inject;
import javax.inject.Singleton;

@Slf4j
@Singleton
public class MongoChangeStreamConsumer {
    private final MongoClient mongoClient;
    private final WebSocketManager webSocketManager;
    private final ContextStore contextStore;
    private final Vertx vertx;

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
        MongoCollection<Document> entities = mongoClient
                .getDatabase("asyncloadtest")
                .getCollection("entities");

        // Run the change stream in a Vert.x worker
        vertx.executeBlocking(promise -> {
            try {
                entities.watch().forEach(changeDoc -> {
                    try {
                        processChange(changeDoc);
                    } catch (Exception e) {
                        log.error("Error processing change", e);
                    }
                });
            } catch (Exception e) {
                log.error("Error in change stream", e);
                promise.fail(e);
            }
        });
    }

    private void processChange(ChangeStreamDocument<Document> changeDoc) {
        if (changeDoc.getOperationType() == OperationType.UPDATE ||
                changeDoc.getOperationType() == OperationType.INSERT) {

            Document fullDocument = changeDoc.getFullDocument();
            String entityId = fullDocument.getString("_id");

            JsonObject updateMessage = new JsonObject()
                    .put("type", "entityUpdate")
                    .put("entityId", entityId)
                    .put("version", fullDocument.getLong("version"))
                    .put("state", new JsonObject(fullDocument.get("state", Document.class).toJson()));

            // Find affected channels and broadcast
            contextStore.getChannelsForEntity(entityId)
                    .forEach(channelDoc -> {
                        String channelId = channelDoc.getString("channelId");
                        webSocketManager.broadcastToChannel(channelId, updateMessage);
                    });
        }
    }
}