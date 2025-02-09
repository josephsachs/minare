package com.asyncloadtest.example;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.core.json.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EntitySimulator {
    private static final Logger log = LoggerFactory.getLogger(EntitySimulator.class);
    private final Vertx vertx;
    private final long updateIntervalMs;
    private Long timerId;

    public EntitySimulator(Vertx vertx, long updateIntervalMs) {
        this.vertx = vertx;
        this.updateIntervalMs = updateIntervalMs;
    }

    public void start() {
        // Setup periodic timer for entity updates
        timerId = vertx.setPeriodic(updateIntervalMs, id -> {
            generateEntityUpdate();
        });
        log.info("Entity simulator started with update interval: {}ms", updateIntervalMs);
    }

    public void stop() {
        if (timerId != null) {
            vertx.cancelTimer(timerId);
            timerId = null;
        }
        log.info("Entity simulator stopped");
    }

    private void generateEntityUpdate() {
        // For now, just generate random entity state changes
        // This would be replaced with actual game/application logic
        JsonObject update = new JsonObject()
                .put("type", "entityUpdate")
                .put("timestamp", System.currentTimeMillis())
                .put("entities", generateRandomEntities());

        // Publish to event bus for WebSocketVerticle to handle
        vertx.eventBus().publish("entity.updates", update);
    }

    private JsonArray generateRandomEntities() {
        // Mock entity generation logic
        JsonArray entities = new JsonArray();
        // Add some random entities...
        return entities;
    }
}