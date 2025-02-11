// controllers/AbstractEntityController.java
package com.asyncloadtest.controller;

import com.asyncloadtest.persistence.EntityStore;
import io.vertx.core.json.JsonObject;
import javax.inject.Inject;

public abstract class AbstractEntityController {
    private final EntityStore entityStore;
    protected abstract boolean hasAccess(String connectionId, String entityId);
    protected abstract boolean validateUpdate(JsonObject currentState, JsonObject proposedUpdate);

    @Inject
    public AbstractEntityController(EntityStore entityStore) {
        this.entityStore = entityStore;
    }

    public void handleUpdate(String entityId, JsonObject update, long version) {
        JsonObject current = entityStore.getEntity(entityId);
        if (current == null) {
            throw new IllegalStateException("Entity not found");
        }

        if (!validateUpdate(current, update)) {
            throw new IllegalStateException("Invalid update");
        }

        try {
            entityStore.updateEntity(entityId, version, update);
        } catch (IllegalStateException e) {
            throw new IllegalStateException("Entity was modified by another request");
        }
    }
}